import datetime
from enum import Enum
from astropy.time import Time

locations = {2: "acacia_mwaingest", 3: "banksia", 4: "acacia_mwa"}


class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18


def get_gpstime(date_time: datetime.datetime) -> int:
    gpstime = Time(date_time)
    gpstime.format = "gps"
    return gpstime.value
