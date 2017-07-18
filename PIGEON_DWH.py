import csv
import scipy
from dateutil.parser import parse
import datetime as dt
import pandas as pd
import numpy as np
import sys, getopt
import os
import ConfigParser
from Constants import *
from collections import defaultdict
from sklearn import preprocessing

from dateutil import rrule
from datetime import timedelta
from scipy.stats import ttest_ind, ttest_1samp
from statsmodels.tsa.seasonal import seasonal_decompose
import scipy

class PIGEON(object):
    def __init__(self, config_path):
        self._config = self.__read_config(config_path)
        self._model = PIGEON_MODEL(self._config)

    def initialize(self):
        # check INPUT_DIR
        INPUT_DIR = self._config['INPUT_DIR']
        if not self.__check_file_exists(INPUT_DIR):
            return False

        if not self.__check_output_directory():
            return False

        if not self.__check_logging_system():
            return False

        if not self.__validate_input():
            return False

        return True

    def transform_input(self):
        self._model.create_data_index()

    def run_model(self):
        return self._model.run_model()

    def create_report(self, report_file):
        params = {}
        params[K_CONFIDENCE] = 100 - self._config[K_CONFIDENCE]
        params[K_SEASONAL] = self._config[K_SEASONAL]

        dump_report(self._config['OUTPUT_DIR'],report_file, params)

    def finalize(self):
        return True

    ##### Private method ####
    def __check_file_exists(self, file_path):
        return os.path.exists(file_path)

    def __validate_input(self):
        return True

    def __check_logging_system(self):
        return True

    def __check_output_directory(self):
        out_dir = self._config['OUTPUT_DIR']
        return os.path.isdir(out_dir)

    def __read_config(self, config_path):
        if not self.__check_file_exists(config_path):
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
        config['INPUT_DIR'] = Config.get('Parameters', 'INPUT_DIR')
        config['OUTPUT_DIR'] = Config.get('Parameters', 'OUTPUT_DIR')
        config[K_CONFIDENCE] = int(Config.get('Parameters', K_CONFIDENCE))
        config[K_SEASONAL] = float(Config.get('Parameters', K_SEASONAL))

        return config

class PIGEON_MODEL(object):
    def __init__(self, config):
        self._config = config
        self._acc_merchant = defaultdict(set)
        self._acc_monthly = defaultdict(set)
        self._acc_merc_monthly = defaultdict(set)
        self._acc_merc_yearly = defaultdict(set)
        self._acc_groupby_monthly = defaultdict(list)
        self._acc_groupby_merc_monthly = defaultdict(list)
        self._acc_groupby_merc_yearly = defaultdict(list)
        self._acc_merc_txn = defaultdict(list)
        #self._acc_merc_txn_budget = defaultdict(list)
        self._acc_merc_freq = defaultdict(list)
        self._acc_merc_visit_monthly = defaultdict(list)
        self._month_keys = []
        self._STA_CATS = set(['PETROL'])

        start_date, end_date = self._config[K_DATE_RANGE]
        self._running_month = end_date.month

    def create_data_index(self):
        #dwh_acc_merchant = defaultdict(set)
        #dwh_acc_monthly = defaultdict(set)
        #dwh_acc_merc_monthly = defaultdict(set)
        #dwh_acc_groupby_merc_monthly = defaultdict(list)
        #dwh_acc_merc_txn = defaultdict(list)
        start_date, end_date = self._config[K_DATE_RANGE]

        month_keys = []
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
            v = dt.year*100 + dt.month
            month_keys.append(v)
        self._month_keys = month_keys

        tmp_acc_groupby_merc_yearly = defaultdict(list)

        data_path = self._config['INPUT_DIR']
        with open(data_path, 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter='|')
            next(spamreader, None)
            for k in spamreader:
                #key = "_".join((k[0],k[2]))
                #txn_date = parse(k[1]).date()
                #print "_".join((k[0],k[2]))
                txn_timestamp = int(k[4])
                self._acc_merchant[k[0]].add(k[2])
                self._acc_monthly[k[0]].add(int(k[4]))
                self._acc_groupby_monthly["_".join((k[0],k[4]))].append(float(k[3]))
                #self._acc_merc_txn_reduce[k[0]].append(float(k[3]))
                self._acc_merc_monthly["_".join((k[0],k[2]))].add(int(k[4]))
                #self._acc_merc_yearly["_".join((k[0],k[2]))].add(int(k[4])/100)
                self._acc_groupby_merc_monthly["_".join((k[0],k[2],str(int(k[4]))))].append(float(k[3]))
                #self._acc_groupby_merc_yearly["_".join((k[0],k[2],str(int(k[4])/100)))].append(float(k[3]))
                #tmp_acc_groupby_merc_yearly["_".join((k[0],k[2],str(int(k[4])/100)))].append(float(k[3]))
                self._acc_merc_txn["_".join((k[0],k[2], "H" if txn_timestamp < 201704 else "R"))].append(float(k[3]))
                self._acc_merc_freq["_".join((k[0],k[2]))].append(1)
                #print k
                #print ', '.join(row)
                #break

        # for k,v in self._acc_groupby_merc_yearly.iteritems():
        #     self._acc_groupby_merc_yearly[k] = np.mean(v)

        # reduce
        for k,v in self._acc_groupby_merc_monthly.iteritems():
            self._acc_groupby_merc_monthly[k] = sum(v)
            self._acc_merc_visit_monthly["_".join(k.split("_")[:2])].append(len(v))

        for k,v in self._acc_merc_freq.iteritems():
            self._acc_merc_freq[k] = sum(v)

        for k,v in self._acc_groupby_monthly.iteritems():
            self._acc_groupby_monthly[k] = sum(v)

        for k,v in self._acc_merc_visit_monthly.iteritems():
            print k,v
            break

    def run_model(self):
        acc_keys = self._acc_merchant.keys()

        res = {}
        for acc_number in acc_keys:

            acc_data = self.prepare_data(acc_number)
            #print acc_data

            report = self.get_bahaviour_scores(acc_data)
            res[acc_number] = report

            #print report
            #break
            #t,p = ttest_ind(tst, trn, equal_var=False)
        return res

    def prepare_data(self, acc_number):
        merchants = self._acc_merchant[acc_number]
        month_keys = self._month_keys
        STA_CATS = self._STA_CATS
        data = {}
        acc_lifetime = 0.0 # get acc lifetime
        #data['Info'] = {}
        #data['Data'] = {}

        #prepare overall trend
        m_keys = self._acc_monthly[acc_number]
        total_monthly = []
        for yyyymm in month_keys:
            if yyyymm in m_keys:
                total_monthly.append(self._acc_groupby_monthly["_".join((acc_number,str(yyyymm)))])
            else:
                total_monthly.append(0)

        non_zero_n = np.count_nonzero(total_monthly) * 100 / float(len(total_monthly))
        if non_zero_n >= 80:
            data['overall'] = total_monthly

        #######################

        data_merc = {}
        for merc in merchants:
            #pass
            freq = self._acc_merc_freq['_'.join([acc_number,merc])]

            if freq < 12:
                continue

            data_merc[merc] = {}
            #total_budget += self._acc_merc_txn_reduce['_'.join([acc_number,merc])]
            acc_merc_key = '_'.join([acc_number,merc])
            m_keys = self._acc_merc_monthly[acc_merc_key]
            first_month = min(m_keys)
            first_month_index = 0
            amts = []
            for i,m in enumerate(month_keys):
                if m in m_keys:
                    amts.append(self._acc_groupby_merc_monthly['_'.join([acc_merc_key,str(m)])])

                    if first_month == m:
                        first_month_index = i
                else:
                    amts.append(0)

            non_zero_n = np.count_nonzero(amts) * 100 / float(len(amts))
            #print first_month, merc, non_zero_n
            if merc in STA_CATS:
                #pass
                srs = pd.Series(amts)
                #print srs
                d_mva = srs.rolling(window=12,center=False).mean()
                #print d_mva
                ts_log_moving_avg_diff = srs - d_mva
                ts_log_moving_avg_diff.dropna(inplace=True)
                #print ts_log_moving_avg_diff.head(12)
                expwighted_avg = srs.ewm(halflife=12,ignore_na=False,min_periods=0,adjust=True).mean()
                ts_log_ewma_diff = srs - expwighted_avg

                amts = ts_log_ewma_diff.values.tolist()

                ts_data = amts[first_month_index:]
                data_merc[merc]['txn'] = (ts_data[:-1], ts_data[-1:])
            else:
                data_merc[merc]['txn'] = self.get_historical_data(acc_number,merc)

            if non_zero_n >= 80.0:
                data_merc[merc]['txn_ss'] = amts

        data['Data'] = data_merc

        return data

    def get_historical_data(self, acc_number, merc):
        acc_merc_H = '_'.join([acc_number,merc,'H'])
        acc_merc_R = '_'.join([acc_number,merc,'R'])
        trn = []
        tst = []
        if acc_merc_H in self._acc_merc_txn:
            trn = self._acc_merc_txn[acc_merc_H]
        if acc_merc_R in self._acc_merc_txn:
            tst = self._acc_merc_txn[acc_merc_R]

        return (trn, tst)

    def get_bahaviour_scores(self, data_info):
        # get lifetime
        #acc_lifetime = 0.0
        acc_data = data_info['Data']
        res = {}
        month_i = self._running_month - 1
        for merc, dataDic in acc_data.iteritems():
            data = {}
            txn_data = dataDic['txn']
            (score, chance) = t_test(txn_data[1], txn_data[0])
            data['B-SCORE'] = (score, chance)

            if 'txn_ss' in dataDic:
                ts_data = dataDic['txn_ss']
                s_score = compute_seasonal_score(ts_data, month_i)
                data['S-SCORE'] = s_score

            res[merc] = data

        if 'overall' in data_info:
            ts_data = data_info['overall']
            if ts_data.count(ts_data[0]) != len(ts_data): # check list identication, all values are the same
                s_scores = compute_seasonal_score(ts_data, None)
                res['overall'] = s_scores

        return res

eps = 0.0001
def t_test(TST,TR):
    if len(TR) < 1:
        return (-1, -1)

    if len(TST) < 1:
        return (-1, 0.0)

    if np.count_nonzero(TR) == 0 and np.count_nonzero(TST) == 0:
        return (-1, -1)

    #A, B = list(TR), list(TST)
    #return (-1, -1)
    t, p = 0, 0
    if len(TST) == 1:
        n = len(TR)
        u = np.mean(TR)
        std = np.std(TR)

        if std < eps:
            diff = (TST[0] - u)
            if diff > eps:
                t = 1
            elif diff < -1*eps:
                t = -1
            else:
                return (-1, -1)

            return (round(t, 4), 0.00)

        t = (TST[0] - u)/std
        p = scipy.stats.t.sf(np.abs(t), n-1)*2
        return (round(t, 4), round(p*100.0, 2))
        #t,p = ttest_1samp(A, TST[0])

        #print "M ", M
    else:
        #print "N ", N
        #print "M ", M
        t,p = ttest_ind(TST, TR, equal_var=False)

    return (round(t, 4), round(p*100.0, 2))

def compute_seasonal_score(data_monthly, month_i=None):
    #print data_monthly
    result = seasonal_decompose(data_monthly, model='additive', freq=12)
    #print np.std(result.seasonal)
    seasonal_scores = result.seasonal[:12]
    scores = [round(i,4) for i in preprocessing.scale(seasonal_scores)]
    return scores if month_i is None else scores[month_i]

def dump_report(to_path, report, params):
    b = 0
    s = 0
    both = 0
    abbrvs = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    rows = []
    behavior_overall = []
    confid = params[K_CONFIDENCE]
    seasonal_threshold = params[K_SEASONAL]
    for acc_number,report in report.iteritems():
        has_b = False
        has_s = False
        has_both = False
        for merc, score_info in report.iteritems():
            if merc == 'overall':
                data = {}
                data['ACC_NUMBER'] = acc_number
                data['ENTITY'] = 'KCC'
                s_scores = score_info
                for i,mon_abbr in enumerate(abbrvs):
                    data[mon_abbr] = s_scores[i]

                behavior_overall.append(data)
            else:
                if 'B-SCORE' in score_info:
                    t,p = score_info['B-SCORE']
                    #print p
                    if p > -1 and p < confid:
                        data = {}
                        data['ACC_NUMBER'] = acc_number
                        data['ENTITY'] = 'KCC'
                        data['MERCHANT'] = merc
                        if t > 0:
                            data['BEHAVIOR_FLAG'] = 'U'
                        else:
                            data['BEHAVIOR_FLAG'] = 'D'
                        #print p
                        has_b = True

                        raw_score = t
                        if 'S-SCORE' in score_info:
                            ss = score_info['S-SCORE']
                            raw_score *= ss
                            has_both = True
                            if ss < seasonal_threshold:
                                has_s = True
                                data['SEASONAL_FLAG'] = 'N'
                            else:
                                data['SEASONAL_FLAG'] = 'Y'
                        else:
                            data['SEASONAL_FLAG'] = 'NA'

                        data['RAW_SCORE'] = round(np.tanh(raw_score), 2)

                        rows.append(data)

        if has_b:
            b += 1
            has_b = False
        if has_s:
            has_s = False
            s += 1
        if has_both:
            has_both = False
            both += 1

    # print n,b,s, both
    #
    # print rows[:5]
    # print behavior_overall[:5]

    columns = ['ACC_NUMBER', 'ENTITY', 'MERCHANT', 'BEHAVIOR_FLAG', 'SEASONAL_FLAG', 'RAW_SCORE']
    with open(os.path.join(to_path, 'behavior_insight.csv'), 'wb') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, columns)
        w.writeheader()
        w.writerows(rows)

    columns = ['ACC_NUMBER', 'ENTITY'] + abbrvs
    with open(os.path.join(to_path, 'behavior_overall.csv'), 'wb') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, columns)
        w.writeheader()
        w.writerows(behavior_overall)
