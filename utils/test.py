import h5coro
from h5coro import filedriver

h5obj = h5coro.H5Coro("/data/GEDI/GEDI02_A_2020243072712_O09719_01_T09028_02_003_01_V002.h5", filedriver.FileDriver)
