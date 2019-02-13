from enum import Enum


class CSVType(Enum):
    Sea = 0
    Air = 1
    Tunnel = 2
    Shift = 3
    NonResponse = 4
    Unsampled = 5
    Survey = 6
