# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Literal

import fsspec

from .path import split_filename


# Note, we could make FileObj an ABC and then introduce concrete
# File and Directory classes which would make code more comprehensible.
# But then, we cannot know the concrete type of the resulting type if we
# append path components, i.e., my_dir / "file_or_dir" = ?


class FileObj:
    """An object that represents a file or directory in some filesystem.

    :param uri: The file or directory URI
    :param storage_options: Optional storage options specific to
        the protocol of the URI
    :param fs: Optional fsspec filesystem instance.
        Use with care, the filesystem must be consistent with *uri*
        and *storage_options*. For internal use only.
    :param path: The path info the filesystem *fs*.
        Use with care, the path must be consistent with *uri*.
        For internal use only.
    """

    def __init__(
        self,
        uri: str,
        storage_options: dict[str, Any] | None = None,
        fs: fsspec.AbstractFileSystem | None = None,
        path: str | None = None,
    ):
        self._uri = uri
        self._storage_options = storage_options
        self._fs = fs
        self._path = path

    def __del__(self):
        """Call ``close()``."""
        self.close()

    def __str__(self):
        return self._uri

    def __repr__(self):
        if self._storage_options is None:
            return f"FileObj({self.uri!r})"
        else:
            return (
                f"FileObj({self.uri!r}," f" storage_options={self._storage_options!r})"
            )

    @property
    def uri(self) -> str:
        """The URI."""
        return self._uri

    @property
    def storage_options(self) -> dict[str, Any] | None:
        """Storage options for creating the filesystem object."""
        return self._storage_options

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """The filesystem."""
        self._resolve()
        return self._fs

    @property
    def path(self) -> str:
        """The path of the file or directory into the filesystem."""
        self._resolve()
        return self._path

    def close(self):
        """Close the filesystem used by this file object."""
        if self._fs is not None:
            if hasattr(self._fs, "close") and callable(self._fs.close):
                self._fs.close()
            self._fs = None

    def __truediv__(self, rel_path: str):
        return self.for_path(rel_path)

    @property
    def filename(self) -> str:
        return split_filename(self.path)[1]

    @property
    def parent(self) -> "FileObj":
        if "::" in self.uri:
            # If uri is a chained URL, use path of first component
            first_uri, rest = self.uri.split("::", maxsplit=1)
            protocol, path = fsspec.core.split_protocol(first_uri)
            parent_path, _ = split_filename(path)
            if protocol:
                new_uri = f"{protocol}://{parent_path}::{rest}"
            else:
                new_uri = f"{parent_path}::{rest}"
        else:
            protocol, path = fsspec.core.split_protocol(self.uri)
            parent_path, _ = split_filename(path)
            if protocol:
                new_uri = f"{protocol}://{parent_path}"
            else:
                new_uri = parent_path

        if self._path is not None:
            new_path, _ = split_filename(self._path)
        else:
            # it is ok, we are still unresolved
            new_path = None

        return FileObj(
            uri=new_uri,
            path=new_path,
            storage_options=self._storage_options,
            fs=self.fs,
        )

    def for_path(self, rel_path: str) -> "FileObj":
        if not isinstance(rel_path, str):
            raise TypeError("rel_path must have type str")
        if rel_path.startswith("/"):
            raise ValueError("rel_path must be relative")

        if not rel_path:
            return self

        if "::" in self.uri:
            # If uri is a chained URL, add path to first component
            first_uri, rest = self.uri.split("::", maxsplit=1)
            new_uri = f"{first_uri}/{rel_path}::{rest}"
        else:
            new_uri = f"{self.uri}/{rel_path}"

        if self._path is not None:
            new_path = f"{self._path.rstrip('/')}/{rel_path}"
        else:
            # it is ok, we are still unresolved
            new_path = None

        return FileObj(
            uri=new_uri,
            path=new_path,
            storage_options=self._storage_options,
            fs=self.fs,
        )

    ############################################################
    # Basic filesystem operations

    def exists(self) -> bool:
        self._resolve()
        return self._fs.exists(self._path)

    def mkdir(self):
        self._resolve()
        self._fs.mkdir(self._path, create_parents=False)

    def read(self, mode: Literal["rb"] | Literal["r"] = "rb") -> bytes | str:
        self._resolve()
        with self._fs.open(self._path, mode=mode) as f:
            return f.read()

    def write(
        self,
        data: str | bytes,
        mode: Literal["wb"] | Literal["w"] | Literal["ab"] | Literal["a"] | None = None,
    ) -> int:
        self._resolve()
        if mode is None:
            mode = "w" if isinstance(data, str) else "wb"
        with self._fs.open(self._path, mode=mode) as f:
            return f.write(data)

    def delete(self, recursive: bool = False) -> bool:
        self._resolve()
        return self._fs.rm(self._path, recursive=recursive)

    ############################################################
    # Internals

    def _resolve(self):
        if self._fs is None or self._path is None:
            fs, path = fsspec.core.url_to_fs(self._uri, **(self._storage_options or {}))
            if self._fs is None:
                self._fs = fs
            if self._path is None:
                self._path = path
