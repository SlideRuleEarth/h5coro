import numpy as np

ATLAS_SDP_EPOCH_GPS = 1198800018.0
GPS_EPOCH_START = 315964800.0
LEAP_SECS_AT_GPS_EPOCH = 10.0
LEAP_SECONDS = [
    2272060800, # 1 Jan 1972
    2287785600, # 1 Jul 1972
    2303683200, # 1 Jan 1973
    2335219200, # 1 Jan 1974
    2366755200, # 1 Jan 1975
    2398291200, # 1 Jan 1976
    2429913600, # 1 Jan 1977
    2461449600, # 1 Jan 1978
    2492985600, # 1 Jan 1979
    2524521600, # 1 Jan 1980
    2571782400, # 1 Jul 1981
    2603318400, # 1 Jul 1982
    2634854400, # 1 Jul 1983
    2698012800, # 1 Jul 1985
    2776982400, # 1 Jan 1988
    2840140800, # 1 Jan 1990
    2871676800, # 1 Jan 1991
    2918937600, # 1 Jul 1992
    2950473600, # 1 Jul 1993
    2982009600, # 1 Jul 1994
    3029443200, # 1 Jan 1996
    3076704000, # 1 Jul 1997
    3124137600, # 1 Jan 1999
    3345062400, # 1 Jan 2006
    3439756800, # 1 Jan 2009
    3550089600, # 1 Jul 2012
    3644697600, # 1 Jul 2015
    3692217600, # 1 Jan 2017
]

def leap_secs(sys_secs, start_secs):
    start_index = len(LEAP_SECONDS)
    current_index = 0
    for i in range(len(LEAP_SECONDS) - 1, 0, -1):
        if (sys_secs > LEAP_SECONDS[i]):
            current_index = i
            break
    if start_secs == GPS_EPOCH_START:
       start_index = LEAP_SECS_AT_GPS_EPOCH
    else:
       for i in range(len(LEAP_SECONDS)):
            if (start_secs < LEAP_SECONDS[i]):
                start_index = i
                break
    return ((current_index - start_index) + 1)

def to_timestamp(delta_time):
    gps_secs = delta_time + ATLAS_SDP_EPOCH_GPS
    sys_secs = gps_secs + GPS_EPOCH_START
    sys_secs -= leap_secs(sys_secs, GPS_EPOCH_START);
    return sys_secs

def to_datetime(delta_time):
    timestamps_ns = [to_timestamp(dt) * 1000000000.0 for dt in delta_time]
    return np.array(timestamps_ns).astype('datetime64[ns]')
