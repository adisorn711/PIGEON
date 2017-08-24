#!/usr/bin/python

import sys, getopt
import os
import ConfigParser
import datetime as dt
import pandas as pd
import multiprocessing
import pickle
from timeit import default_timer as timer
from Constants import *

def combine_scores(report):
    pass

def create_report(report):
    # result = combine_scores('partitions')
    # for acc_res in result.iteritems():
    #     print acc_res
    #     break
    pass

import PIGEON_DWH

def __check_file_exists(file_path):
    return os.path.exists(file_path)

def __read_config(config_path):
    if not __check_file_exists(config_path):
        print "Config file does not exists."
        sys.exit(2)

    Config = ConfigParser.ConfigParser()
    Config.read(config_path)
    config = {}

    start_dates = map(int, Config.get('Parameters', 'START_DATE').split("-"))
    end_dates = map(int, Config.get('Parameters', 'END_DATE').split("-"))

    start_date = dt.datetime(start_dates[0], start_dates[1], 1).date()
    end_date = dt.datetime(end_dates[0], end_dates[1], 1).date()
    #date_ranges = ((start_date.year, start_date.month),(end_date.year, end_date.month))

    config[K_DATE_RANGE] = (start_date, end_date)
    config['EXEC_DATE'] = str(int(end_date.year*100) + int(end_date.month))

    config['INPUT_DIR'] = Config.get('Parameters', 'INPUT_DIR')
    config['OUTPUT_DIR'] = Config.get('Parameters', 'OUTPUT_DIR')
    config[K_CONFIDENCE] = int(Config.get('Parameters', K_CONFIDENCE))
    config[K_SEASONAL] = float(Config.get('Parameters', K_SEASONAL))
    config['DB_DIR'] = Config.get('Parameters', 'DB_DIR')
    config['SAVE_EVERY'] = int(Config.get('Parameters', 'SAVE_EVERY'))
    config['LOGGING_DIR'] = Config.get('Parameters', 'LOGGING_DIR')

    return config

def run_pigeon(config_file):
    config = __read_config('./config.ini')

    pigeon = PIGEON_DWH.PIGEON(config)
    if not pigeon.initialize():
        print "Initialization failed"
        sys.exit(2)

    #pigeon.transform_input()

    pigeon.run_model()

    #pigeon.create_report(res)

    pigeon.finalize()

def main(argv):
    if len(argv) == 0:
        print 'PIGEON.py -c <config_file>'
        sys.exit(2)

    config_file = ''
    data_file = ''
    try:
       opts, args = getopt.getopt(argv,"hc:i:",["cfile="])
    except getopt.GetoptError:
       print 'PIGEON.py -c <config_file>'
       sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
           print 'PIGEON.py -c <config_file>'
           sys.exit()
        elif opt in ("-c", "--cfile"):
           config_file = arg

    print 'Proceed the task with configuration "{}"'.format(config_file)

    run_pigeon(config_file)


if __name__ == '__main__':
    main(sys.argv[1:])
