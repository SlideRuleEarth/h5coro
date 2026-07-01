import pytest
import random
from types import SimpleNamespace
from h5coro.h5dataset import H5Dataset, FatalError

# Reference implementation: the element-wise loop that shuffleChunk replaced.
# Kept here so the vectorized version stays byte-identical to it.
def reference_shuffle(input, output_offset, output_size, type_size):
    output = bytearray(output_size)
    dst_index = 0
    shuffle_block_size = int(len(input) / type_size)
    elements_to_shuffle = int(output_size / type_size)
    start_element = int(output_offset / type_size)
    for element_index in range(start_element, start_element + elements_to_shuffle):
        for val_index in range(0, type_size):
            src_index = (val_index * shuffle_block_size) + element_index
            output[dst_index] = input[src_index]
            dst_index += 1
    return output

def make_dataset(errorChecking=True):
    """Build a minimal object exposing what shuffleChunk needs."""
    dataset = SimpleNamespace(resourceObject=SimpleNamespace(errorChecking=errorChecking))
    dataset.shuffleChunk = H5Dataset.shuffleChunk.__get__(dataset)
    return dataset

@pytest.mark.parametrize("type_size", [1, 2, 4, 8])
class TestShuffleChunk:
    def test_full_chunk(self, type_size):
        elements = 1000
        data = bytes(random.getrandbits(8) for _ in range(elements * type_size))
        dataset = make_dataset()
        result = dataset.shuffleChunk(data, 0, len(data), type_size)
        assert bytes(result) == bytes(reference_shuffle(data, 0, len(data), type_size))

    def test_windowed_reads(self, type_size):
        elements = 997
        data = bytes(random.getrandbits(8) for _ in range(elements * type_size))
        dataset = make_dataset()
        for start, count in [(0, 1), (1, 1), (0, elements), (13, 250), (elements - 1, 1)]:
            offset = start * type_size
            size = count * type_size
            result = dataset.shuffleChunk(data, offset, size, type_size)
            assert bytes(result) == bytes(reference_shuffle(data, offset, size, type_size))

    def test_ragged_input_length(self, type_size):
        # chunk length not divisible by type_size: trailing bytes are ignored
        elements = 100
        data = bytes(random.getrandbits(8) for _ in range(elements * type_size + type_size - 1))
        dataset = make_dataset()
        result = dataset.shuffleChunk(data, 0, elements * type_size, type_size)
        assert bytes(result) == bytes(reference_shuffle(data, 0, elements * type_size, type_size))

    def test_ragged_output_size(self, type_size):
        # output_size not divisible by type_size: tail is zero-padded
        elements = 100
        data = bytes(random.getrandbits(8) for _ in range(elements * type_size))
        dataset = make_dataset()
        size = 10 * type_size + (type_size - 1)
        result = dataset.shuffleChunk(data, 0, size, type_size)
        assert len(result) == size
        assert bytes(result) == bytes(reference_shuffle(data, 0, size, type_size))

    def test_zero_elements(self, type_size):
        data = bytes(range(type_size)) * 16
        dataset = make_dataset()
        result = dataset.shuffleChunk(data, 0, 0, type_size)
        assert bytes(result) == b''

    def test_window_past_chunk_raises(self, type_size):
        elements = 10
        data = bytes(elements * type_size)
        dataset = make_dataset()
        with pytest.raises(FatalError):
            dataset.shuffleChunk(data, 5 * type_size, 6 * type_size, type_size)

def test_known_pattern():
    # bytes of element e at input[v * block + e]: two uint32 elements
    # 0x03020100 and 0x07060504 shuffle to lo-bytes then hi-bytes
    shuffled = bytes([0x00, 0x04, 0x01, 0x05, 0x02, 0x06, 0x03, 0x07])
    dataset = make_dataset()
    result = dataset.shuffleChunk(shuffled, 0, 8, 4)
    assert bytes(result) == bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07])

def test_invalid_type_size():
    dataset = make_dataset()
    with pytest.raises(FatalError):
        dataset.shuffleChunk(bytes(8), 0, 8, 9)
