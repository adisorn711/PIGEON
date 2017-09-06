from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import pandas as pd
import numpy as np
from datetime import *
from dateutil.parser import parse
from dateutil import rrule
from datetime import timedelta
from scipy.stats import ttest_ind, ttest_1samp
import scipy
from sklearn import preprocessing
from statsmodels.tsa.seasonal import seasonal_decompose
from pyspark import SparkFiles
import ConfigParser
import os

ctx = SparkContext("local[*]", "PIGEON SWIFT")
#ctx = SparkContext(scfg)

dwhRaw = ctx.textFile("file:///home/ks11174695/Desktop/crm/notebooks/KCC_TXN_FULL.tsv")
ctx.addFile("file:///home/ks11174695/Desktop/PIGEON/config.ini")

print datetime.now()
config_path = SparkFiles.get('config.ini')
with open(SparkFiles.get('config.ini')) as test_file:
    print test_file.read()

print type(config_path), config_path
Config = ConfigParser.ConfigParser()
Config.read(config_path)

print os.path.exists(config_path)

start_dates = map(int, Config.get('Parameters', 'START_DATE').split("-"))
end_dates = map(int, Config.get('Parameters', 'END_DATE').split("-"))

start_date = datetime(start_dates[0], start_dates[1], 1).date()
end_date = datetime(end_dates[0], end_dates[1], 1).date()

month_keys = []
for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
    v = dt.year*100 + dt.month
    month_keys.append(v)

print month_keys

mm2index = dict([(v,i) for i,v in enumerate(month_keys)])

print mm2index

def custom_lambda(x):
    values = [0] * len(month_keys)
    values[mm2index[x[4]]] = x[3]

    txn_tup = ([x[3]],[],np.array(values)) if x[4] < 201704 else ([],[x[3]],np.array(values))
    
    return ((x[0],x[2]), txn_tup)

print dwhRaw.getNumPartitions()
dwhFile = dwhRaw
print dwhFile
#dwhHeader = dwhFile.filter(lambda l: "ACCOUNT_KEY" in l)
dwhContents = dwhFile

dwh_temp = dwhContents.map(lambda k: k.replace('"','').split("\t"))
dwh_temp = dwh_temp.map(lambda p: (p[0], p[1], p[2], float(p[3]), int(p[4]) ))
dwh_temp = dwh_temp.filter(lambda arr: arr[4] >= 201401 and arr[4] <= 201704)


STAs = set(['PETROL'])


rdd_txn_temp = dwh_temp.filter(lambda p: p[2] not in STAs)
rdd_txn_temp = rdd_txn_temp.map(lambda p: ((p[0],p[2]),([],[])))
#print rdd_txn_temp.take(5)
rdd_txn = dwh_temp.map(custom_lambda)
print rdd_txn.take(5)

rdd_txn_final = rdd_txn.reduceByKey(lambda a, b: (a[0]+b[0],a[1]+b[1],a[2]+b[2]))
#rdd_txn_final = rdd_txn_final.map(lambda p: (p[0],(p[1][0],p[1][1], len(p[1][0])+len(p[1][1]))))
print rdd_txn_final.take(5)
exit()

rdd_txn = dwh_temp.map(lambda p: ((p[0],p[2]), ([p[3]],[]) if p[4] < 201704 else ([],[p[3]]) ))
#print rdd_txn.take(5)
print "Pre union"
rdd_txn_final = rdd_txn_temp.union(rdd_txn).reduceByKey(lambda a, b: (a[0]+b[0],a[1]+b[1]))
rdd_txn_final = rdd_txn_final.map(lambda p: (p[0],(p[1][0],p[1][1], len(p[1][0])+len(p[1][1]))))
print rdd_txn_final.take(5)
exit()
eps = 0.0001

def t_test(TST,TR):
    if len(TR) < 1:
        return (-11, -1)

    if len(TST) < 1:
        return (-10, 0.0)

    if np.count_nonzero(TR) == 0 and np.count_nonzero(TST) == 0:
        return (-20, -1)


    if set(TST) == set(TR):
        return (-13, -1)

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

def compute_behaviour(x):
    tr = x[1][0]
    tst = x[1][1]
    
    x1, x2 = t_test(tst,tr)
    return (x[0], np.array([x1,x2,0]))

#filter category less used
rdd_txn_final_computed = rdd_txn_final.map(compute_behaviour)

def map_seasonality(x):
    values = [0] * len(month_keys)
    values[mm2index[x[4]]] = x[3]
    
    return ((x[0],x[2]), np.array(values))

def compute_seasonality(x):
    txn = x[1]
    
    ratio = np.count_nonzero(txn) / float(len(txn))
    
    if ratio < 0.8:
        return (x[0], np.array([0,0,-99]))
        
    #print data_monthly
    result = seasonal_decompose(txn, model='additive', freq=12)
    #print np.std(result.seasonal)
    
    seasonal_scores = result.seasonal[:12]
    scores = [round(i,4) for i in preprocessing.scale(seasonal_scores)]
    
    return (x[0], np.array([0,0,scores[0]]))

def map_to_str(arr):
    return map(str,arr)

rdd_txn_ss_temp = dwh_temp.filter(lambda p: p[2] not in STAs)
rdd_txn_seasonal = rdd_txn_ss_temp.map(map_seasonality).reduceByKey(lambda a,b: a+b).map(compute_seasonality)
rdd_txn_behaviour_final = rdd_txn_seasonal.union(rdd_txn_final_computed).reduceByKey(lambda a,b: a+b)
rdd_txn_behaviour_final_unpacked = rdd_txn_behaviour_final.map(\
lambda x: "|".join(map_to_str((x[0][0], x[0][1], round(x[1][0], 4), round(x[1][1], 2), round(x[1][2], 4)))))
rdd_txn_behaviour_final_unpacked.saveAsTextFile('./rdd_txn_behaviour_final')
print datetime.now()
exit()

def custom_lambda_sta(x):
    values = [0] * len(month_keys)
    values[mm2index[x[4]]] = x[3]
    
    return ((x[0], x[2]), np.array(values))

def fill_ts(ts):
    latest_nonzero = None
    for i,v in enumerate(ts):
        if v == 0 and latest_nonzero is not None:
            ts[i] = latest_nonzero

        if v != 0:
            latest_nonzero = v

    return ts

def complete_ts(p):
    x = p[1]
    ts = fill_ts(x)
    reversed_ts = ts[::-1]

    reversed_ts = fill_ts(reversed_ts)
    ts = reversed_ts[::-1]
    
    return (p[0], ts)

rdd_txn_stationary = dwh_temp.filter(lambda p: p[2] in STAs)
rdd_txn_stationary_temp = rdd_txn_stationary.map(custom_lambda_sta).reduceByKey(lambda a, b: a+b)
rdd_txn_stationary_filled = rdd_txn_stationary_temp.filter(lambda p: np.count_nonzero(p[1]) > 30)

def compute_rolling_mean(x):
    ts = x[1]
    srs = pd.Series(ts)
    l,u = srs.quantile(0.25), srs.quantile(0.75)

    ts_filtered = [i for i in ts if i >= l and i <= u]
    #print srs
    d_mva = srs.rolling(window=6,center=False).mean()

    d_std = np.std(ts_filtered)

    #d_mvstd = d_mva + 2.0*d_std
    #d_mvstd_lower = d_mva - 2.0*d_std
    
    return (x[0], x[1][-1], d_mva.tolist()[-1], d_std)


rdd_txn_stationary_computed = rdd_txn_stationary_filled.map(compute_rolling_mean)

def detect_down_up(x):
    x_i = x[1]
    d_mva = x[2]
    std = x[3]
    
    d_mvstd = d_mva + 2.0*std
    d_mvstd_lower = d_mva - 2.0*std
    
    flag = "N"
    if x_i < d_mvstd_lower:
        flag = "D"
    elif x_i > d_mvstd:
        flag = "U"
        
    return (x[0], flag)
    
rdd_txn_stationary_downup = rdd_txn_stationary_computed.map(detect_down_up)


ctx.stop()