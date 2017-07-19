#!/usr/bin/python

import sqlite3
import os
from Constants import *

class PGDB(object):
    def __init__(self, config):
        self._config = config
        self._running_month = ""
        self._table_name = ""

    def initialize(self):
        db_root_dir = self._config['DB_DIR']
        self._conn = sqlite3.connect(os.path.join(db_root_dir,'account.db'))

        start_date, end_date = self._config[K_DATE_RANGE]
        self._running_month = int(end_date.year*100) + int(end_date.month)
        self._table_name = "ACCOUNT_{}".format(self._running_month)

        self._conn.execute("CREATE TABLE IF NOT EXISTS {} \
             (ACC_NUMBER CHAR(16) PRIMARY KEY NOT NULL);".format(self._table_name))

        cursor = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_tables = [i[0] for i in cursor.fetchall()]

        for t in all_tables:
            if t != self._table_name:
                self._conn.execute("DROP TABLE IF EXISTS {};".format(t))

        return True

    def finalize(self):
        self._conn.close()

    def save_accounts(self, acc_numbers):
        #print acc_numbers
        items = [(acc,) for acc in acc_numbers]
        #print items
        self._conn.executemany('insert into {} values (?)'.format(self._table_name), items)
        self._conn.commit()

    def has_account(self, acc_number):
        cursor = self._conn.execute("SELECT ACC_NUMBER \
        FROM {} WHERE ACC_NUMBER={};".format(self._table_name, acc_number))

        return cursor.fetchone() is not None

    def get_all_accounts(self):
        cursor = self._conn.execute("SELECT ACC_NUMBER \
        FROM {};".format(self._table_name))

        return set([row[0] for row in cursor.fetchall()])
