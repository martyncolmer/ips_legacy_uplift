import sqlite3


class SQLConnection:
    __instance = None
    __conn = None

    @staticmethod
    def getinstance():
        if SQLConnection.__instance is None:
            SQLConnection()
        return SQLConnection.__instance

    @staticmethod
    def connection():
        return SQLConnection.__instance.__conn

    def __init__(self):
        if SQLConnection.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SQLConnection.__instance = self
            SQLConnection.__conn = sqlite3.connect(":memory:")


