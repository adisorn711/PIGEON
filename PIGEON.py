#!/usr/bin/python

import sys, getopt
import os
import ConfigParser
import datetime as dt
import pandas as pd
import multiprocessing
import pickle
from timeit import default_timer as timer


def read_config(config_path):
    if not os.path.exists(config_path):
        print "Config file does not exists."
        sys.exit(2)

    Config = ConfigParser.ConfigParser()
    Config.read(config_path)
    config = {}

    date_range_txt = Config.get('Parameters', 'date_range')
    min_range = date_range_txt.split('|')[0]
    max_range = date_range_txt.split('|')[1]

    start_date = dt.datetime.strptime(min_range, "%Y-%m-%d").date()
    end_date = dt.datetime.strptime(max_range, "%Y-%m-%d").date()
    date_ranges = ((start_date.year, start_date.month),(end_date.year, end_date.month))

    config[K_DATE_RANGE] = date_ranges
    config[K_CONFIDENCE] = int(Config.get('Parameters', K_CONFIDENCE))
    config[K_SEASONAL] = float(Config.get('Parameters', K_SEASONAL))
    config[K_DATE_RANGE] = date_ranges
    config[K_EXECUTION_MODE] = Config.get('Mode', K_EXECUTION_MODE)

    return config

def run_task(data_path, config):
    full_txn = pd.read_csv(data_path, sep='|', parse_dates=[1])
    setting = {K_DATE_RANGE: config[K_DATE_RANGE]}
    dwh = CustomerDWH(full_txn, setting)

    allAccounts = dwh.get_all_customers()
    allcategories = dwh.get_all_categories()

    print len(allAccounts)
    print len(allcategories)

    i = 0
    accs = []
    for acc in allAccounts:
        acc_data = dwh.get_customer_data(acc)
        accs.append(acc_data)
        #usage = acc_data.usage_by_category()
        #bav_res = acc_data.get_bahaviour_scores(ofCategory=usage)
        #end = timer()
        #print(end - start)
        #break
        if (i+1) % 1000 == 0:
            print "Finished {}".format(i+1)

        i += 1

    compute_scores(accs, cpu_cores=4)

def split_data(x,n_chunks):
    size = len(x)/n_chunks
    xx = [x[i:i+size] for i  in range(0, len(x), size)]

    if len(xx) != n_chunks:
        xx[-2] = xx[-2] + xx[-1]
        xx = xx[:-1]
        #pass

    return xx

def compute_scores(accs, cpu_cores=1):
    def get_score(accs, i):
        res = []
        for acc_data in accs:
            usage = acc_data.usage_by_category()
            bav_res = acc_data.get_bahaviour_scores(ofCategory=usage)
            res.append({acc_data.account_key: bav_res})

        print "Finished {} with len = {}".format(i, len(res))
        output = open('partitions/data_{}.pkl'.format(i), 'wb')
        pickle.dump(res, output)
        output.close()

    chunks = split_data(accs[:10000],cpu_cores)
    start = timer()
    jobs = []
    for i in range(0, cpu_cores):
        process = multiprocessing.Process(target=get_score,
                                    args=(chunks[i], i))
        #all_list.extend(out_list)
        jobs.append(process)

    # Start the processes (i.e. calculate the random number lists)
    for j in jobs:
        j.start()

    # Ensure all of the processes have finished
    for j in jobs:
        j.join()

    print "List processing complete."
    end = timer()
    print(end - start)

def combine_scores(out_path):
    results = []
    for f in os.listdir(out_path):
        pkl_file = open(os.path.join(out_path,f), 'rb')

        data = pickle.load(pkl_file)
        results.extend(data)
        pkl_file.close()

    print len(results)

    from collections import Counter

    cnt = 0
    shouldCount = False
    cats_down = []
    cats_up = []
    accs_down = set()
    accs_up = set()
    log_results = {}
    for rep in results:
        for acc_number, info in rep.iteritems():
            #print info
            #scores = rep.values()
            data = {}
            for k,vdic in info.iteritems():
                #print vdic
                v = vdic['B-SCORE']
                s = vdic['S-SCORE']
                if v[1] > 0 and v[1] <= 5.0 and v[0] < 0:
                    #if v[1][1] <= 5:
                    score = {'PERFORMACE': 'U'}
                    if np.absolute(s) <= 0.75:
                        shouldCount = True
                        #print k,v[0][0]
                        cats_down.append(k)
                        accs_down.add(acc_number)
                        #data['UP'].append(k)
                        score['SEASONAL'] = 'N'
                    else:
                        score['SEASONAL'] = 'Y'

                    data[k] = score
                elif v[1] > 0 and v[1] <= 5.0 and v[0] > 0:
                    #if v[1][1] <= 5:
                    score = {'PERFORMACE': 'D'}
                    if np.absolute(s) <= 0.75:
                        shouldCount = True
                        #print k,v[1][1]
                        cats_up.append(k)
                        accs_up.add(acc_number)
                        #data['DOWN'].append(k)
                        score['SEASONAL'] = 'N'
                    else:
                        score['SEASONAL'] = 'Y'
                    data[k] = score

            if shouldCount:
                shouldCount = False
                cnt += 1
                log_results[acc_number] = data
        #print scores

        #break
    print cnt, round(cnt/float(len(results))*100.0, 2)

    actual_down = accs_down.difference(accs_up)
    print len(accs_down)
    print len(accs_up)
    print len(actual_down), round(len(actual_down)/float(len(results))*100.0, 2)
    return actual_down
    return log_results

def combine_scores(report):
    pass

def create_report(report):
    # result = combine_scores('partitions')
    # for acc_res in result.iteritems():
    #     print acc_res
    #     break
    pass

import PIGEON_DWH

def run_pigeon(config_file):
    pigeon = PIGEON_DWH.PIGEON('./config.ini')

    if not pigeon.initialize():
        print "Initialization failed"
        sys.exit(2)

    pigeon.transform_input()

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
