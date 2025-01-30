import pytest
import h5coro
from h5coro import s3driver

HDF_OBJECT = "sliderule/data/test/ATL03_20200401184200_00950707_005_01.h5"

@pytest.mark.region
class TestHDF:
    def test_dataset_read(self):
        dataset1 =  "/gt1l/heights/delta_time"
        dataset2 =  "/gt1l/heights/dist_ph_across"
        dataset3 =  "/gt1l/heights/dist_ph_along"
        dataset4 =  "/gt1l/heights/h_ph"
        dataset5 =  "/gt1l/heights/lat_ph"
        dataset6 =  "/gt1l/heights/lon_ph"
        dataset7 =  "/gt1l/heights/pce_mframe_cnt"
        dataset8 =  "/gt1l/heights/ph_id_channel"
        dataset9 =  "/gt1l/heights/ph_id_count"
        dataset10 = "/gt1l/heights/ph_id_pulse"
        dataset11 = "/gt1l/heights/quality_ph"
        dataset12 = "/gt1l/heights/signal_conf_ph"

        hyperslice = [[0, 5]]
        hyperslice_2D = [[0, 5], [0, 2]]

        # Create a list of datasets to read
        datasets = [
                     {'dataset': dataset1,  'hyperslice': hyperslice},
                     {'dataset': dataset2,  'hyperslice': hyperslice},
                     {'dataset': dataset3,  'hyperslice': hyperslice},
                     {'dataset': dataset4,  'hyperslice': hyperslice},
                     {'dataset': dataset5,  'hyperslice': hyperslice},
                     {'dataset': dataset6,  'hyperslice': hyperslice},
                     {'dataset': dataset7,  'hyperslice': hyperslice},
                     {'dataset': dataset8,  'hyperslice': hyperslice},
                     {'dataset': dataset9,  'hyperslice': hyperslice},
                     {'dataset': dataset10, 'hyperslice': hyperslice},
                     {'dataset': dataset11, 'hyperslice': hyperslice},
                     {'dataset': dataset12, 'hyperslice': hyperslice_2D}
                   ]

        # Initialize the H5Coro object and read the datasets
        h5obj = h5coro.H5Coro(HDF_OBJECT, s3driver.S3Driver, errorChecking=True, multiProcess=False)
        promise = h5obj.readDatasets(datasets, block=True)

        # Print promise information
        for dataset in promise:
            print(f'{dataset}: {promise[dataset]}')
