import logging
import h5coro
from h5coro import filedriver

logging.basicConfig(level=logging.INFO, format='%(created)f %(levelname)-5s [%(filename)s:%(lineno)5d] %(message)s')

h5obj = h5coro.H5Coro("/data/ATLAS/ATL03_20181017222812_02950102_005_01.h5", filedriver.FileDriver, datasets=['/gt2l/heights/h_ph'])

print(h5obj.datasets)
