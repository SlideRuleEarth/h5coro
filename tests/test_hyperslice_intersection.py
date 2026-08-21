import pytest
from types import SimpleNamespace
from h5coro.h5dataset import H5Dataset


def make_dataset(ndims, hyperslice):
    """Build a minimal object exposing what hypersliceIntersection (node_level==0) needs."""
    dataset = SimpleNamespace(meta=SimpleNamespace(ndims=ndims), hyperslice=hyperslice)
    dataset.hypersliceIntersection = H5Dataset.hypersliceIntersection.__get__(dataset)
    return dataset


class TestHypersliceIntersection1D:
    # chunkDimensions = [2123], hyperslice reads 10 elements starting at `start`.
    # node_slice for the chunk containing `start` boundary is half-open [c0, c0+2123).
    CHUNK = 2123

    def chunk_slice(self, chunk_index):
        c0 = chunk_index * self.CHUNK
        return [(c0, c0 + self.CHUNK)]

    def test_chunk_ending_at_hyperslice_start_excluded(self):
        # Regression: a chunk that ends exactly where the read begins contributes
        # nothing and must NOT be reported as intersecting.
        for start in (self.CHUNK, 5 * self.CHUNK, 24 * self.CHUNK):
            dataset = make_dataset(1, [(start, start + 10)])
            preceding_chunk = self.chunk_slice(start // self.CHUNK - 1)
            assert preceding_chunk[0][1] == start  # sanity: ends exactly at start
            assert dataset.hypersliceIntersection(preceding_chunk, 0) is False

    def test_chunk_containing_start_included(self):
        for start in (self.CHUNK, 5 * self.CHUNK, 24 * self.CHUNK):
            dataset = make_dataset(1, [(start, start + 10)])
            containing_chunk = self.chunk_slice(start // self.CHUNK)
            assert dataset.hypersliceIntersection(containing_chunk, 0) is True

    def test_boundary_off_by_one_either_side(self):
        # start one element before the boundary: preceding chunk genuinely overlaps
        start = 5 * self.CHUNK - 1
        dataset = make_dataset(1, [(start, start + 10)])
        preceding_chunk = self.chunk_slice(4)
        assert preceding_chunk[0][1] == 5 * self.CHUNK
        assert dataset.hypersliceIntersection(preceding_chunk, 0) is True

        # start one element after the boundary: preceding chunk does not overlap
        start = 5 * self.CHUNK + 1
        dataset = make_dataset(1, [(start, start + 10)])
        assert dataset.hypersliceIntersection(self.chunk_slice(4), 0) is False
        assert dataset.hypersliceIntersection(self.chunk_slice(5), 0) is True

    def test_chunk_starting_at_hyperslice_end_excluded(self):
        # symmetric high-bound: a chunk starting exactly at the hyperslice end
        # (already correct before the fix) contributes nothing.
        start = 5 * self.CHUNK
        dataset = make_dataset(1, [(start, start + 10)])
        following_chunk = [(start + 10, start + 10 + self.CHUNK)]
        assert dataset.hypersliceIntersection(following_chunk, 0) is False


class TestHypersliceIntersection2D:
    # chunk touching the boundary in only one dimension must be excluded.
    def test_boundary_in_one_dimension_excluded(self):
        # hyperslice: dim0 [100, 110), dim1 [50, 60)
        dataset = make_dataset(2, [(100, 110), (50, 60)])
        # chunk overlaps dim1 but ends exactly at dim0 start -> no overlap
        node_slice = [(90, 100), (50, 60)]
        assert dataset.hypersliceIntersection(node_slice, 0) is False

    def test_overlap_in_all_dimensions_included(self):
        dataset = make_dataset(2, [(100, 110), (50, 60)])
        node_slice = [(95, 105), (55, 65)]
        assert dataset.hypersliceIntersection(node_slice, 0) is True
