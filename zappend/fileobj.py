from typing import Any

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
        self._filesystem: fsspec.AbstractFileSystem | None = None
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
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """The filesystem."""
        self._resolve()
        return self._filesystem

    @property
    def path(self) -> str:
        """The path of the file or directory into the filesystem."""
        self._resolve()
        return self._path

    def close(self):
        """Close the filesystem used by this file object."""
        if self._filesystem is not None:
            if hasattr(self._filesystem, "close") and callable(self._filesystem.close):
                self._filesystem.close()
            self._filesystem = None

    def _resolve(self):
        if self._filesystem is None or self._path is None:
            self._filesystem, self._path = fsspec.core.url_to_fs(
                self._uri, **(self._storage_options or {})
            )

    def for_suffix(self, suffix: str) -> "FileObj":
        suffix = "/" + suffix.strip("/")
        # TODO: Fix adding suffixes for chained URLs.
        #   Adding the suffix to a chained URL may create an invalid URI,
        #   for example, adding suffix ".zarray" to chained URL
        #   "zip://chl::/users/forman/test.zip" produces
        #   "zip://chl::/users/forman/test.zip/.zarray" instead of
        #   "zip://chl/.zarray::/users/forman/test.zip".
        fo = FileObj(self.uri + suffix)
        fo._filesystem = self._filesystem
        fo._storage_options = self._storage_options
        if fo._path is not None:
            fo._path += suffix
        return fo
