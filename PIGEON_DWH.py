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
import logging
import hashlib

from dateutil import rrule
from datetime import timedelta
from scipy.stats import ttest_ind, ttest_1samp
from statsmodels.tsa.seasonal import seasonal_decompose
import scipy
from PIGEON_Datastore import PGDB



class PIGEON(object):
    def __init__(self, config_path):
        #self._config = self.__read_config(config_path)
        self._config = config_path

        self.__check_logging_system()

        self._db = PGDB(self._config)
        self._model = PIGEON_MODEL(self._config)

    def initialize(self):
        # check INPUT_DIR
        INPUT_DIR = self._config['INPUT_DIR']
        if not self.__check_file_exists(INPUT_DIR):
            return False

        if not self.__check_output_directory():
            return False

        if not self.__validate_input():
            return False

        if not self._db.initialize():
            return False

        logging.info('Initialization completed')

        return True

    def transform_input(self):
        # get done accounts
        logging.info('Retreiving all saved accounts')
        saved_accs = self._db.get_all_accounts()
        logging.info("{} accounts have been done.".format(len(saved_accs)))

        logging.info('Creating data structure and index before start running')
        self._model.create_data_index(saved_accs)
        logging.info('Data Index has been completed.')

    def run_model(self):

        logging.info('Retreiving all saved accounts')
        saved_accs = self._db.get_all_accounts()
        logging.info("{} accounts have been done.".format(len(saved_accs)))

        logging.info('Executing Model')
        self._model.run_batch(self.__exec_callback, self._db, saved_accs)
        logging.info('Finished Model Execution')

    def create_report(self, report_file):
        params = {}
        params[K_CONFIDENCE] = 100 - self._config[K_CONFIDENCE]
        params[K_SEASONAL] = self._config[K_SEASONAL]

        dump_report(self._config['OUTPUT_DIR'],report_file, params)

    def finalize(self):
        logging.info('Finalizing the system.')
        self._db.finalize()
        logging.info('The system has finished.')

        return True

    ##### Private method ####

    def __exec_callback(self, accs_dic):
        params = {}
        params[K_CONFIDENCE] = 100 - self._config[K_CONFIDENCE]
        params[K_SEASONAL] = self._config[K_SEASONAL]

        dump_report(self._config['OUTPUT_DIR'],accs_dic, params)

        logging.info('{} accounts have been done and saved to persistent store.'.format(len(accs_dic)))

    def __check_file_exists(self, file_path):
        return os.path.exists(file_path)

    def __validate_input(self):
        logging.info('Start validating input')
        return True
        data_path = self._config['INPUT_DIR']

        #df = pd.read_csv(os.path.join(data_path,'input.csv'), sep='|', parse_dates=[1])
        lines = 0
        res = [0,0,0,0,0]
        with open(os.path.join(data_path, 'input.csv'), 'rb') as csvfile:
            next(csvfile, None)
            for l in csvfile:
                row = l.split("|")

                try:
                    int(row[0])
                except:
                    res[0] = 1
                    break
                #dt.datetime.strptime( row[1], "%Y%m%d" )
                try:
                    dt.datetime.strptime( row[1], "%Y%m%d" )
                except:
                    res[1] = 1
                    break

                try:
                    float(row[3])
                except:
                    res[3] = 1
                    break

                if len(row[4].rstrip()) != 6:
                    res[4] = 1
                    break

        if sum(res) != 0:
            logging.info('Input is invalid !!!')
            return False

        logging.info('Input has been successfully validated.')
        return True

    def __check_logging_system(self):
        (start_date, end_date) = self._config[K_DATE_RANGE]
        logAtDate = str(int(end_date.year*100) + int(end_date.month)) + '.log'
        log_dir = self._config['LOGGING_DIR']
        log_file_path = os.path.join(log_dir, logAtDate)

        logging.basicConfig(filename=log_file_path, filemode='a', level=logging.INFO, format='%(asctime)s-%(levelname)s-: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

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
        config['EXEC_DATE'] = str(int(end_date.year*100) + int(end_date.month))

        config['INPUT_DIR'] = Config.get('Parameters', 'INPUT_DIR')
        config['OUTPUT_DIR'] = Config.get('Parameters', 'OUTPUT_DIR')
        config[K_CONFIDENCE] = int(Config.get('Parameters', K_CONFIDENCE))
        config[K_SEASONAL] = float(Config.get('Parameters', K_SEASONAL))
        config['DB_DIR'] = Config.get('Parameters', 'DB_DIR')
        config['SAVE_EVERY'] = int(Config.get('Parameters', 'SAVE_EVERY'))
        config['LOGGING_DIR'] = Config.get('Parameters', 'LOGGING_DIR')

        return config

class PIGEON_MODEL(object):
    def __init__(self, config):
        self._config = config

        # self._acc_merchant = defaultdict(set)
        # self._acc_monthly = defaultdict(set)
        # self._acc_merc_monthly = defaultdict(set)
        # #self._acc_merc_yearly = defaultdict(set)
        # self._acc_groupby_monthly = defaultdict(list)
        # self._acc_groupby_merc_monthly = defaultdict(list)
        # #self._acc_groupby_merc_yearly = defaultdict(list)
        # self._acc_merc_txn = defaultdict(list)
        # #self._acc_merc_txn_budget = defaultdict(list)
        # self._acc_merc_freq = defaultdict(list)
        # self._acc_merc_visit_monthly = defaultdict(list)

        self._acc_merchant = dict()
        self._acc_monthly = dict()
        self._acc_merc_monthly = dict()
        self._acc_groupby_monthly = dict()
        self._acc_groupby_merc_monthly = dict()
        self._acc_merc_txn = dict()
        self._acc_merc_freq = dict()
        self._acc_merc_visit_monthly = dict()

        self._acc_master = dict()
        self._month_keys = []
        self._STA_CATS = set(['PETROL'])
        self._current_month_index = 0

        start_date, end_date = self._config[K_DATE_RANGE]
        self._running_month = end_date.month

    def create_data_index(self, saved_accs):
        start_date, end_date = self._config[K_DATE_RANGE]

        month_keys = []
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
            v = dt.year*100 + dt.month
            month_keys.append(v)
        self._month_keys = month_keys
        self._current_month_index = self.get_month_index(month_keys)
        left_bound = self._month_keys[0]
        right_bound = self._month_keys[-1]

        tmp_acc_groupby_merc_yearly = defaultdict(list)

        data_path = self._config['INPUT_DIR']
        exec_date = int(self._config['EXEC_DATE'])
        prev_acc = None
        num_acc = 0
        _acc_merchant = dict()

        _acc_merchant = dict()
        _acc_monthly = dict()
        _acc_groupby_monthly = dict()
        _acc_merc_monthly = dict()
        _acc_groupby_merc_monthly = dict()
        _acc_merc_txn = dict()
        _acc_merc_freq = dict()

        row_count = 0
        with open(os.path.join(data_path, 'input.csv'), 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter='|')
            next(spamreader, None)
            seen_accs = list()
            for k in spamreader:
                #k = map(lambda x:x.strip(), row)
                if k[0] in saved_accs:
                    continue
                
                txn_timestamp = int(k[4])

                if txn_timestamp < left_bound or txn_timestamp > right_bound:
                    continue

                cur_acc = k[0]

                if prev_acc is not None and prev_acc != cur_acc:
                    num_acc += 1

                if num_acc == 10000:
                    #seen_accs.extend(self._acc_merchant.keys())
                    self.post_indexing()
                    print dt.now(), num_acc, row_count
                    yield self._acc_merchant.keys()
                    self.clear_data_index()
                    num_acc = 0

                
                a = "%s_%s" % (k[0],k[4]) #k[0] + '_' + k[4]
                b = "%s_%s" % (k[0],k[2]) #k[0] + '_' + k[2]
                c = "%s_%s_%s" % (k[0],k[2],k[4]) #k[0] + '_' + k[2] + '_' + k[4]
                txn_timestamp = int(k[4])

                try:
                    self._acc_merchant[k[0]].add(k[2])
                except KeyError:
                    self._acc_merchant[k[0]] = set([k[2]])

                try:
                    self._acc_monthly[k[0]].add(int(k[4]))
                except KeyError:
                    self._acc_monthly[k[0]] = set([int(k[4])])

                try:
                    self._acc_groupby_monthly[a].append(float(k[3]))
                except KeyError:
                    self._acc_groupby_monthly[a] = [float(k[3]),]

                try:
                    self._acc_merc_monthly[b].add(int(k[4]))
                except KeyError:
                    self._acc_merc_monthly[b] = set([int(k[4])])

                try:
                    self._acc_groupby_merc_monthly[c].append(float(k[3]))
                except KeyError:
                    self._acc_groupby_merc_monthly[c] = [float(k[3]),]

                try:
                    self._acc_merc_freq[b].append(1)
                except KeyError:
                    self._acc_merc_freq[b] = [1]

                e = "%s_%s" % (b,"H" if txn_timestamp < exec_date else "R") # b + '_' + "H" if txn_timestamp < exec_date else "R"

                try:
                    self._acc_merc_txn[e].append(float(k[3]))
                except KeyError:
                    self._acc_merc_txn[e] = [float(k[3]),]

                prev_acc = cur_acc

            if num_acc > 0:
                #seen_accs.extend(self._acc_merchant.keys())
                print num_acc
                self.post_indexing()
                yield self._acc_merchant.keys()
                #print len(self._acc_merchant.keys())
                self.clear_data_index()
                num_acc = 0

            print len(seen_accs), len(set(seen_accs))


    def post_indexing(self):
        for k,v in self._acc_groupby_merc_monthly.iteritems():
            self._acc_groupby_merc_monthly[k] = sum(v)

            try:
                self._acc_merc_visit_monthly["_".join(k.split("_")[:2])].append(len(v))
            except KeyError:
                self._acc_merc_visit_monthly["_".join(k.split("_")[:2])] = [len(v),]

        for k,v in self._acc_merc_freq.iteritems():
            self._acc_merc_freq[k] = sum(v)

        for k,v in self._acc_groupby_monthly.iteritems():
            self._acc_groupby_monthly[k] = sum(v)

    def clear_data_index(self):
        self._acc_merchant = dict()
        self._acc_monthly = dict()
        self._acc_merc_monthly = dict()
        self._acc_groupby_monthly = dict()
        self._acc_groupby_merc_monthly = dict()
        self._acc_merc_txn = dict()
        self._acc_merc_freq = dict()
        self._acc_merc_visit_monthly = dict()


    def run_batch(self, cb, db, saved_accs):
        for acc_keys in self.create_data_index(saved_accs):

            res = {}
            for acc_number in acc_keys:
                acc_data = self.prepare_data(acc_number)

                report = self.get_bahaviour_scores(acc_data)
                res[acc_number] = report

            cb(res)
            db.save_accounts(res.keys())

    def run_model(self, cb, db):
        acc_keys = self._acc_merchant.keys()
        save_every = self._config['SAVE_EVERY']
        res = {}
        i = 1
        for acc_number in acc_keys:
            acc_data = self.prepare_data(acc_number)
            #print acc_data

            report = self.get_bahaviour_scores(acc_data)
            res[acc_number] = report

            if i % save_every == 0:
                cb(res)
                db.save_accounts(res.keys())
                res = {}
                #print "Finished ", i

            i += 1
            #print report
            #break
            #t,p = ttest_ind(tst, trn, equal_var=False)
        if len(res) > 0:
            cb(res)
            db.save_accounts(res.keys())
        #return res

    def prepare_data(self, acc_number):
        merchants = self._acc_merchant[acc_number]
        month_keys = self._month_keys
        STA_CATS = self._STA_CATS
        data = {}
        acc_lifetime = 0.0 # get acc lifetime

        #prepare overall trend
        m_keys = self._acc_monthly[acc_number]
        total_monthly = []
        #print m_keys
        for yyyymm in month_keys:
            if yyyymm in m_keys:
                #print yyyymm, self._acc_groupby_monthly["_".join((acc_number,str(yyyymm)))]
                total_monthly.append(self._acc_groupby_monthly["_".join((acc_number,str(yyyymm)))])
            else:
                total_monthly.append(0)

        #print acc_number
        #print total_monthly
        non_zero_n = np.count_nonzero(total_monthly) * 100 / float(len(total_monthly))
        if non_zero_n >= 80:
            data['overall'] = total_monthly
        data['wallet'] = total_monthly
        #######################

        data_merc = {}
        for merc in merchants:
            #pass
            #freq = self._acc_merc_freq['_'.join([acc_number,merc])]

            #if freq < 12:
                #continue

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
        month_i = self._current_month_index
        for merc, dataDic in acc_data.iteritems():
            data = {}
            txn_data = dataDic['txn']
            (score, chance) = t_test(txn_data[1], txn_data[0])
            data['SP-SCORE'] = score
            data['SP-PROB'] = chance

            if 'txn_ss' in dataDic:
                ts_data = dataDic['txn_ss']
                s_score = compute_seasonal_score(ts_data, month_i)
                data['SS-SCORE'] = s_score

            res[merc] = data

        if 'overall' in data_info:
            ts_data = data_info['overall']
            if ts_data.count(ts_data[0]) != len(ts_data): # check list identication, all values are the same
                s_scores = compute_seasonal_score(ts_data, None)
                s_scores_arranged = self.rearrange_months_arr(s_scores)
                res['overall'] = s_scores_arranged

        if 'wallet' in data_info:
            ts_data = data_info['wallet']
            res['wallet'] = wallet_sense(ts_data)

        return res

    def rearrange_months_arr(self, arr):
        yyyymm = self._month_keys[0]
        mm = (yyyymm%100) - 1

        if mm > 0:
            cut_idx = 12 - mm
            res = arr[cut_idx:] + arr[:cut_idx]

            return res
            
        return arr     

    def get_month_index(self, month_keys):  
        last_m = month_keys[-1]%100

        month_index = 0
        for i,v in enumerate(month_keys):
            m = v%100
            if m % 12 == last_m:
                month_index = i
                break
        
        return month_index

        


eps = 0.0001
def t_test(TST,TR):
    if len(TR) < 1:
        return (10, 0.0) # new comer for that category

    if len(TST) < 1:
        return (-10, 0.0)

    if np.count_nonzero(TR) == 0 and np.count_nonzero(TST) == 0:
        return (-20, -1)

    if set(TST) == set(TR):
        return (11, 100) # same spending from time to time

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
                return (0, 100)

            return (round(t, 4), 0.00)
        #print "Non zero ",std, TST[0]
        t = (TST[0] - u)/std
        p = scipy.stats.t.sf(np.abs(t), n-1)*2
        return (round(t, 4), round(p*100.0, 2))
    elif len(TR) == 1:
        u = np.mean(TST)
        x = TR[0]

        t = -12 if x < u else 12
        p = 0.00
    else:
        #print len(TST), len(TR), type(TST), type(TR)
        #t,p = -5,-5
        t,p = ttest_ind(TST, TR, equal_var=False)

        if np.isinf(t):
            u_tr = np.mean(TR)
            u_tst = np.mean(TST)

            t = -14 if u_tst < u_tr else 14
            p = 0.00

    return (round(t, 4), round(p*100.0, 2))

def compute_seasonal_score(data_monthly, month_i=None):
    #print data_monthly
    result = seasonal_decompose(data_monthly, model='additive', freq=12)
    #print np.std(result.seasonal)
    seasonal_scores = result.seasonal[:12]
    scores = [round(i,4) for i in preprocessing.scale(seasonal_scores)]
    return scores if month_i is None else scores[month_i]

def wallet_sense(data_monthly):
    srs = pd.Series(data_monthly)
    l,u = srs.quantile(0.25), srs.quantile(0.75)
    ts_filtered = [i for i in data_monthly if i >= l and i <= u]

    d_mva = srs.rolling(window=6,center=False).mean()
    d_std = np.std(ts_filtered)
    last_spend = data_monthly[-1]
    u = d_mva.tolist()[-1]

    d_mvstd = u + 2.0*d_std
    d_mvstd_lower = u - 2.0*d_std

    if last_spend < d_mvstd_lower:
        return 0
    elif last_spend > d_mvstd:
        return 2

    return 1 

def dump_report(to_path, report_res, params):
    b = 0
    s = 0
    both = 0
    abbrvs = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    rows = []
    behavior_overall = []
    confid = params[K_CONFIDENCE]
    seasonal_threshold = params[K_SEASONAL]
    individuals = []
    for acc_number,report in report_res.iteritems():
        has_b = False
        has_s = False
        has_both = False
        #print report
        for merc, score_info in report.iteritems():
            if merc == 'overall':
                data = {}
                data['ACC_NUMBER'] = acc_number
                s_scores = score_info
                for i,mon_abbr in enumerate(abbrvs):
                    data[mon_abbr] = s_scores[i]

                behavior_overall.append(data)
            elif merc == 'wallet':
                data = {}
                data['ACC_NUMBER'] = acc_number
                data['SENSE'] = report['wallet']
                individuals.append(data)
            else:
                data = {}
                data['ACC_NUMBER'] = acc_number
                data['MERCHANT'] = merc
                data['SP-SCORE'] = score_info['SP-SCORE']
                data['SP-CONF'] = score_info['SP-PROB']
                if 'SS-SCORE' in score_info:
                    data['SS-SCORE'] = score_info['SS-SCORE']
                else:
                    data['SS-SCORE'] = 404
            
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

    columns = ['ACC_NUMBER', 'MERCHANT', 'SP-SCORE', 'SP-CONF', 'SS-SCORE']
    wrtie_to_csv(os.path.join(to_path, 'spending_category.csv'), columns, rows)

    columns = ['ACC_NUMBER'] + abbrvs
    wrtie_to_csv(os.path.join(to_path, 'spending_overall.csv'), columns, behavior_overall)

    columns = ['ACC_NUMBER', 'SENSE']
    wrtie_to_csv(os.path.join(to_path, 'spending_sense.csv'), columns, individuals)

def wrtie_to_csv(path, columns, row_data):
    exists = os.path.exists(path)

    if exists:
        with open(path, 'a') as f:  # Just use 'w' mode in 3.x
            w = csv.DictWriter(f, columns, delimiter='|')
            w.writerows(row_data)

    else:
        with open(path, 'wb') as f:  # Just use 'w' mode in 3.x
            w = csv.DictWriter(f, columns, delimiter='|')
            w.writeheader()
            w.writerows(row_data)
