import mwa_utils
import datetime


def test_get_gpstime():
    assert mwa_utils.get_gpstime(datetime.datetime(2024, 1, 1, 0, 0, 0, 0)) == 1388102418
