import numpy as np
import xarray as xr
from xarray.backends import BackendArray
from xarray.core.indexing import ExplicitIndexer, OuterIndexer, BasicIndexer
import threading

class LazyH5Dataset:
    """Lazy loading dataset proxy using asynchronous H5Promise."""

    def __init__(self, dataset_name, shape, dtype):
        """Initialize using metadata only."""
        self.dataset_name = dataset_name
        self.shape = shape
        self.dtype = dtype
        self.promise = None    # Will be assigned later for actual data
        self.ds_values = None  # Lazy storage for fetched data
        self.lock = threading.Lock()

    def set_promise(self, promise):
        """Assign the full promise for actual data reading."""
        self.promise = promise

    def read(self):
        """Trigger full data read only when accessed."""
        if self.ds_values is not None:
            return self.ds_values

        if self.promise is None:
            raise RuntimeError(f"[{self.dataset_name}] No promise assigned.")

        with self.lock:
            if self.ds_values is None:
                self.ds_values = self.promise[self.dataset_name]
                if self.ds_values is None:
                    raise RuntimeError(f"[{self.dataset_name}] Promise completed but returned no data.")

        return self.ds_values

    @property
    def was_read(self):
        return self.ds_values is not None

    @property
    def size(self):
        """Ensure `.size` returns total number of elements."""
        return np.prod(self.shape) if self.shape is not None else 0

    def __getitem__(self, key):
        """Lazy slice access."""
        return self.read()[key]

    def __array__(self, dtype=None):
        """Prevent NumPy conversion."""
        raise RuntimeError("LazyH5Dataset does not support direct NumPy conversion. Use .read() instead.")

    def release(self):
        """Drop the reference to shared memory data if it was used."""
        self.ds_values = None



class LazyBackendArray(BackendArray):
    """Xarray-compatible lazy array that prevents eager reading."""

    def __init__(self, lazy_ds):
        self.lazy_ds = lazy_ds

    @property
    def shape(self):
        """Ensure xarray recognizes the correct shape."""
        return self.lazy_ds.shape

    @property
    def dtype(self):
        """Ensure xarray recognizes the correct dtype."""
        return self.lazy_ds.dtype

    @property
    def ndim(self):
        """Ensure xarray recognizes the number of dimensions."""
        return len(self.lazy_ds.shape) if self.lazy_ds.shape is not None else 0

    @property
    def size(self):
        """Ensure `.size` returns total number of elements."""
        return np.prod(self.lazy_ds.shape) if self.lazy_ds.shape is not None else 0

    @property
    def nbytes(self):
        return self.size * self.dtype.itemsize

    @property
    def values(self):
        """Ensure `.values` retrieves the lazy-loaded data."""
        return self.lazy_ds.read()

    @property
    def oindex(self):
        # This makes xarray use __getitem__ with OuterIndexer
        return self

    def __getitem__(self, key):
        """Lazy indexing—data is only loaded when accessed."""
        data = self.lazy_ds.read()  # Get the full dataset

         # Handle xarray-style indexers
        if isinstance(key, (ExplicitIndexer, OuterIndexer, BasicIndexer)):
            key = key.tuple  # Extract tuple of slices or indices

        return data[key]  # Apply the correct slice

    def __array__(self, dtype=None):
        """Prevent NumPy conversion."""
        raise RuntimeError("LazyBackendArray does not support direct NumPy conversion. Use .read() instead.")