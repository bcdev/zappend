# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import itertools
import math
from typing import Iterator


def get_chunk_update_range(size: int,
                           chunk_size: int,
                           append_size: int) -> tuple[bool, tuple[int, int]]:
    """Return the range of indexes of affected chunks if a
    given *size* with chunking *chunk_size* is extended by
    *append_size*. The first chunk may be updated or created,
    subsequent chunks would always need to be created.

    :param size: the size of the append dimension
    :param append_size: the size to be appended
    :param chunk_size: the chunk size of the append dimension
    :return: a tuple of the form
        (*first_is_update*, *chunk_index_range*).
    """
    start = size // chunk_size
    pixel = start * chunk_size
    first_is_update = pixel < size <= pixel + chunk_size
    end = math.ceil((size + append_size) / chunk_size)
    return first_is_update, (start, end)


def get_chunk_indices(
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
    append_axis: int,
    append_dim_range: tuple[int, int]
) -> Iterator[tuple[int, ...]]:
    """Get the chunk indices for a dataset of given *shape* and chunk sizes
    *chunks* where the append axis given by *append_axis* is limited to
    the given chunk index range *append_dim_range*.
    """
    dim_ranges = [range(0, math.ceil(s / c)) for s, c in zip(shape, chunks)]
    dim_ranges[append_axis] = range(*append_dim_range)
    return itertools.product(*dim_ranges)
