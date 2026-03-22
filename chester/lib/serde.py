"""
Tools for storing/fetching intermediate results from temporal jobs.

I'm using the local filesystem to smulate a database and/or S3.
Use your imagination - it would be easy enough to hook something like this up.
"""

import pickle
from pathlib import Path
from typing import Any

from chester.constants import CHESTER_CACHE_DIR

_DEFAULT_CACHE_DIR = Path(CHESTER_CACHE_DIR)


class PickleStore:
    """Persists arbitrary Python objects to disk via pickle.

    Files are stored as ``{cache_dir}/{key}.pkl``.  The cache directory is
    created automatically on first use.

    Args:
        cache_dir: Directory where pickle files are written.  Defaults to
            :data:`chester.constants.CHESTER_CACHE_DIR`.
    """

    def __init__(self, cache_dir: Path | str = _DEFAULT_CACHE_DIR) -> None:
        self._cache_dir = Path(cache_dir)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _validate_key(self, key: str) -> None:
        """Reject keys that could cause path traversal."""
        if not key or "/" in key or "\\" in key or ".." in key:
            raise ValueError(
                f"Invalid key {key!r}: keys must not contain '/', '\\\\', or '..'"
            )

    def _path(self, key: str) -> Path:
        self._validate_key(key)
        return self._cache_dir / f"{key}.pkl"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, key: str, obj: Any) -> str:
        """Serialize *obj* to disk under *key* and return the written path."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._path(key)
        with path.open("wb") as fh:
            pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
        return key

    def load(self, key: str) -> Any:
        """Deserialize and return the object stored under *key*.

        Raises:
            FileNotFoundError: if no file exists for *key*.
        """
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"No cached object found for key {key!r} at {path}")
        with path.open("rb") as fh:
            return pickle.load(fh)  # noqa: S301 — trusted local files only

    def exists(self, key: str) -> bool:
        """Return ``True`` if a file exists for *key*, ``False`` otherwise."""
        return self._path(key).exists()

    def delete(self, key: str) -> None:
        """Remove the file for *key* from disk.

        Raises:
            FileNotFoundError: if no file exists for *key*.
        """
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"No cached object found for key {key!r} at {path}")
        path.unlink()


class TextStore:
    """Persists plain-text strings to disk.

    Files are stored as ``{cache_dir}/{key}.txt``.  The cache directory is
    created automatically on first use.

    Args:
        cache_dir: Directory where text files are written.  Defaults to
            :data:`chester.constants.CHESTER_CACHE_DIR`.
        encoding: File encoding to use (default: ``"utf-8"``).
    """

    def __init__(
        self,
        cache_dir: Path | str = _DEFAULT_CACHE_DIR,
        encoding: str = "utf-8",
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._encoding = encoding

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _validate_key(self, key: str) -> None:
        if not key or "/" in key or "\\" in key or ".." in key:
            raise ValueError(
                f"Invalid key {key!r}: keys must not contain '/', '\\\\', or '..'"
            )

    def _path(self, key: str) -> Path:
        self._validate_key(key)
        return self._cache_dir / f"{key}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, key: str, text: str) -> str:
        """Write *text* to disk under *key* and return the key."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._path(key).write_text(text, encoding=self._encoding)
        return key

    def load(self, key: str) -> str:
        """Read and return the string stored under *key*.

        Raises:
            FileNotFoundError: if no file exists for *key*.
        """
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"No cached text found for key {key!r} at {path}")
        return path.read_text(encoding=self._encoding)

    def exists(self, key: str) -> bool:
        """Return ``True`` if a file exists for *key*, ``False`` otherwise."""
        return self._path(key).exists()

    def delete(self, key: str) -> None:
        """Remove the file for *key* from disk.

        Raises:
            FileNotFoundError: if no file exists for *key*.
        """
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"No cached text found for key {key!r} at {path}")
        path.unlink()
