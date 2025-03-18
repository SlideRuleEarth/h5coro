import numpy as np
import xarray as xr
from xarray.backends import BackendArray
from xarray.core.indexing import ExplicitIndexer

class LazyH5Dataset:
    """Lazy loading dataset proxy using asynchronous H5Promise."""

    def __init__(self, dataset_name, shape, dtype):
        """Initialize using metadata only."""
        self.dataset_name = dataset_name
        self.shape = shape
        self.dtype = dtype
        self.promise = None    # Will be assigned later for actual data
        self.ds_values = None  # Lazy storage for fetched data

    def set_promise(self, promise):
        """Assign the full promise for actual data reading."""
        self.promise = promise

    def read(self):
        """Trigger full data read only when accessed."""
        if self.ds_values is None:
            if self.promise is None:
                raise RuntimeError(f"Full data for {self.dataset_name} is not available yet.")
            self.ds_values = self.promise[self.dataset_name]
        return self.ds_values

    @property
    def values(self):
        """Ensure `.values` retrieves the lazy-loaded data."""
        return self.read()  # Fetch data when accessed

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



class LazyXarrayBackendArray(BackendArray):
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
    def values(self):
        """Ensure `.values` retrieves the lazy-loaded data."""
        return self.lazy_ds.read()  # Fetch data when accessed

    def __getitem__(self, key):
        """Lazy indexing—data is only loaded when accessed."""
        data = self.lazy_ds.read()  # Get the full dataset

        # Convert xarray BasicIndexer to a NumPy-friendly index
        if isinstance(key, ExplicitIndexer):
            key = key.tuple  # Convert to a tuple of slices

        return data[key]  # Apply the correct slice

    def __array__(self, dtype=None):
         """Allow NumPy conversion only when explicitly needed."""
         return np.asarray(self.lazy_ds.read(), dtype=dtype)


class LazyBackendArray:
    """Xarray-compatible backend array for LazyH5Dataset that prevents forced reads."""

    def __init__(self, lazy_ds):
        self.lazy_ds = lazy_ds
        self.array = LazyXarrayBackendArray(lazy_ds)  # Properly wrap for Xarray backend

    @property
    def shape(self):
        """Ensure xarray recognizes the correct shape."""
        return self.array.shape

    @property
    def dtype(self):
        """Ensure xarray recognizes the correct dtype."""
        return self.array.dtype

    @property
    def size(self):
        """Ensure `.size` returns total number of elements."""
        return self.array.size

    @property
    def nbytes(self):
        """Ensure xarray can compute memory usage."""
        return self.array.nbytes

    @property
    def values(self):
        """Ensure `.values` retrieves the lazy-loaded data."""
        return self.array.values

    def __getitem__(self, key):
        """Lazy slice access."""
        return self.array[key]

    def __array__(self, dtype=None):
        """Prevent NumPy conversion."""
        raise RuntimeError("LazyBackendArray does not support direct NumPy conversion. Use .read() instead.")

    def to_xarray_lazy(self):
        """Return an xarray-compatible object that prevents NumPy conversion."""
        return self.array  # Use LazyXarrayBackendArray
