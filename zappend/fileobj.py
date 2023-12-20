from typing import Any, Literal

import fsspec


class FileObj:
    """An object that represents a file or directory in some filesystem.

    :param uri: The file or directory URI
    :param storage_options: Optional storage options specific to
        the protocol of the URI
    """

    def __init__(self,
                 uri: str,
                 storage_options: dict[str, Any] | None = None):
        self._uri = uri
        self._storage_options = dict(storage_options or {})
        self._fs: fsspec.AbstractFileSystem | None = None
        self._path: str | None = None

    def __del__(self):
        """Call ``close()``."""
        self.close()

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
            if hasattr(self._fs, "close") and callable(
                    self._fs.close):
                self._fs.close()
            self._fs = None

    def __truediv__(self, rel_path: str):
        return self.for_path(rel_path)

    def for_path(self, rel_path: str) -> "FileObj":
        if not isinstance(rel_path, str):
            raise TypeError("rel_path must have type str")
        if rel_path.startswith("/"):
            raise ValueError("rel_path must be relative")

        old_uri = self.uri
        if "::" in old_uri:
            # If uri is a chained URL, add path to first component
            first, rest = old_uri.split("::", maxsplit=1)
            new_uri = f"{first}/{rel_path}::{rest}"
        else:
            new_uri = f"{old_uri}/{rel_path}"

        old_path = self._path
        if old_path is not None:
            new_path = f"{old_path.rstrip('/')}/{rel_path}"
        else:
            new_path = old_path

        fo = FileObj(new_uri)
        # patch new fo
        fo._storage_options = self._storage_options
        fo._path = new_path
        fo._fs = self._fs

        return fo

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

    def write(self,
              data: str | bytes,
              mode: Literal["wb"]
                    | Literal["w"]
                    | Literal["ab"]
                    | Literal["a"]
                    | None = None) -> int:
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
            fs, path = fsspec.core.url_to_fs(
                self._uri, **(self._storage_options or {})
            )
            if self._fs is None:
                self._fs = fs
            if self._path is None:
                self._path = path
