import logging as log
import sqlite3
from sqlite3 import OperationalError

from config import DATABASE


def initialize_db_from_file(filename):
    conn = sqlite3.connect(DATABASE)
    log.info("Initializing DB.")
    fd = open(filename, 'r')
    for command in fd.read().split(';'):
        try:
            conn.execute(command)
        except OperationalError:
            log.error("Command execution failed.")
    fd.close()
    conn.commit()
    conn.close()
