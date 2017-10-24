from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import pandas as pd
import numpy as np
import math
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
import sys

ctx = SparkContext("local[*]", "PIGEON SWIFT")
sparkSess = SparkSession(ctx)
#ctx = SparkContext(scfg)

data_path = sys.argv[1]
config_path = sys.argv[2]

dwhRaw = ctx.textFile(data_path)
ctx.addFile(config_path)

print datetime.now()
config_path = SparkFiles.get('config.ini')
# with open(SparkFiles.get('config.ini')) as test_file:
#     print test_file.read()

Config = ConfigParser.ConfigParser()
Config.read(config_path)

cutpoint = ctx.broadcast(-2)

print os.path.exists(config_path)

sep = Config.get('Parameters', 'COL_SEP')

start_dates = map(int, Config.get('Parameters', 'START_DATE').split("-"))
end_dates = map(int, Config.get('Parameters', 'END_DATE').split("-"))

start_date = datetime(start_dates[0], start_dates[1], 1).date()
end_date = datetime(end_dates[0], end_dates[1], 1).date()

month_keys = []
for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
    v = dt.year*100 + dt.month
    month_keys.append(v)

print month_keys

month_numbers = [m % 100 for m in month_keys[:12]]
month_numbers_bc = ctx.broadcast(month_numbers)
month_index = ctx.broadcast(month_numbers.index(month_keys[-1] % 100))
print month_index.value

month_number_2_name = ctx.broadcast({1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR'\
, 5: 'MAY',6: 'JUN',7: 'JUL', 8: 'AUG'\
, 9: 'SEP',10: 'OCT', 11: 'NOV', 12: 'DEC'})

monthkey_cut = ctx.broadcast(month_keys[cutpoint.value])

month_keys_bc = ctx.broadcast(month_keys)
n_months = len(month_keys)
n_months_bc = ctx.broadcast(n_months)

mm2index = dict([(v,i) for i,v in enumerate(month_keys)])

print mm2index

mm2index_bc = ctx.broadcast(mm2index)

score_codes = ctx.broadcast(set([-10,10,-14,11,14]))
behaviour_types_map = ctx.broadcast({-10: 301, 10: 201, -14: 302, 11: 202\
, 14: 203})

#
# Customized Lambda functions
#
#
def format_month_to_array(tu):
    arr = ([tu[2]],[]) if tu[1] < monthkey_cut.value else ([],[tu[2]])
    
    return (tu[0], tu[1], arr)

def format_month_to_array_cat(tu):
    arr = [0] * n_months_bc.value
    arr[mm2index_bc.value[tu[2]]] = tu[3]

    return (tu[0], tu[1], tu[2], np.array(arr))

def format_month_to_array_monthly(tu):
    arr = [0] * n_months_bc.value
    arr[mm2index_bc.value[tu[1]]] = tu[2]

    return (tu[0], np.array(arr))
#
#   Computational Models
#
def ttest(B,A):
    TST = B[np.nonzero(B)]
    TR = A[np.nonzero(A)]
    if len(TR) < 1 and len(TST) > 0:
        return (10, 0.0) # new comer for that category

    if len(TST) < 1 and len(TR) > 0:
        return (-10, 0.0)

    if set(TST) == set(TR):
        return (11, 100) # same spending from time to time

    t, p = 0, 0
    if len(TST) == 1:
        n = len(TR)
        u = np.mean(TR)
        std = np.std(TR)

        if np.isclose(0,std):
            if TST[0] > u:
                t = 1
            elif TST[0] < u:
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

def wallet_sense(tu):
    a = np.array(tu[0])
    b = np.array(tu[1])
    TST = b[np.nonzero(b)]
    TR = a[np.nonzero(a)]

    if len(TR) < 1 and len(TST) > 0:
        return 2

    if len(TST) < 1 and len(TR) > 0:
        return 0

    if set(TST) == set(TR):
        return 1
    
    recent = np.mean(b)
    std_h = np.std(TR)
    u_h = np.mean(TR)

    if np.isclose(0,std_h):
        if recent > u_h:
            return 2
        elif recent < u_h:
            return 0
        else:
            return 1

    t_score = (recent - u_h) / std_h
    if t_score > 2.0:
        return 2
    elif t_score < -2.0:
        return 0
    
    return 1

def compute_wallet_sense(tu):
    wallet_res = wallet_sense(tu[1])

    return (tu[0], wallet_res)

def compute_seasonal_score(tu):
    data_monthly = tu[1]
    usage_ratio = round(np.count_nonzero(data_monthly) / float(len(data_monthly)), 4)
    #print data_monthly
    result = seasonal_decompose(data_monthly, model='additive', freq=12)
    #print np.std(result.seasonal)
    seasonal_scores = result.seasonal[:12]
    scores = [round(i,4) for i in preprocessing.scale(seasonal_scores)]

    # transform score to text
    for i,v in enumerate(scores):
        if math.fabs(v) > 1.5:
            if v < 0:
                scores[i] = "LO"
            else:
                scores[i] = "HI"
        else:
            scores[i] = "MED"

    score_dic = dict([(month_number_2_name.value[month_numbers_bc.value[i]],v) for i,v in enumerate(scores)])
    score_dic['ACC_KEY'] = tu[0]
    score_dic['USAGE_RATIO'] = usage_ratio
    return score_dic

def compute_seasonal_score_category(data_monthly):
    usage_ratio = round(np.count_nonzero(data_monthly) / float(len(data_monthly)), 4)
    #print data_monthly
    result = seasonal_decompose(data_monthly, model='additive', freq=12)
    #print np.std(result.seasonal)
    seasonal_scores = result.seasonal[:12]
    scores = [round(i,4) for i in preprocessing.scale(seasonal_scores)]
    return (scores, usage_ratio)

def compute_cateogry_score(tu):
    scores = ttest(tu[1][cutpoint.value:], tu[1][:cutpoint.value])
    #seasonal_scores = compute_seasonal_score_category(tu[1])

    #add behaviour type from t_score
    score = scores[0]
    score_type = behaviour_types_map.value[score] if score in score_codes.value else 100
    pv = scores[1]
    scores = [0,pv,score_type]
    if pv < 0.1:
        if score < 0:
            scores[0] = 0
        else:
            scores[0] = 2
    else:
        scores[0] = 1

    # ss = seasonal_scores[0]
    # ss_dic = {}
    # for i,s_score in enumerate(ss):
    #     month_key = month_number_2_name.value[month_numbers_bc.value[i]]
    #     if math.fabs(s_score) > 1.5:
    #         if s_score < 0:
    #             ss_dic[month_key] = "YD"
    #         else:
    #             ss_dic[month_key] = "YU"
    #     else:
    #         ss_dic[month_key] = "N" 

    row_dic = {}
    row_dic['ACC_KEY'] = tu[0][0]
    row_dic['CATEGORY'] = index2merchant_bc.value[tu[0][1]]
    row_dic['SPENDING_FLAG'] = scores[0]
    row_dic['PVALUE'] = scores[1]
    row_dic['SPENDING_TYPE'] = scores[2]
    #row_dic['USAGE_RATIO'] = seasonal_scores[1]
    #row_dic.update(ss_dic)

    return row_dic

def compute_cateogry_seasonaity(tu):
    seasonal_scores = compute_seasonal_score_category(tu[1])

    ss = seasonal_scores[0]
    ss_dic = {}
    for i,s_score in enumerate(ss):
        month_key = month_number_2_name.value[month_numbers_bc.value[i]]
        if math.fabs(s_score) > 1.5:
            if s_score < 0:
                ss_dic[month_key] = "YD"
            else:
                ss_dic[month_key] = "YU"
        else:
            ss_dic[month_key] = "N" 

    row_dic = {}
    row_dic['ACC_KEY'] = tu[0][0]
    row_dic['CATEGORY'] = index2merchant_bc.value[tu[0][1]]
    row_dic['USAGE_RATIO'] = seasonal_scores[1]
    row_dic.update(ss_dic)

    return row_dic

dwhContents = dwhRaw.filter(lambda p: "ACCOUNT_KEY" not in p)\
.map(lambda k: k.replace('"','').split(sep))\
.filter(lambda p: int(p[4]) >= month_keys_bc.value[0] and int(p[4]) <= month_keys_bc.value[-1]).cache()

sense_rdd = dwhContents.map(lambda p: ((int(p[0]), int(p[4])), float(p[3])))\
.reduceByKey(lambda a,b: a+b)\
.map(lambda p: (p[0][0], p[0][1], p[1]))\
.map(format_month_to_array)\
.map(lambda p: (p[0],p[2]))\
.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
.map(compute_wallet_sense)\
.map(lambda p: Row(ACC_KEY=p[0], ACCOUNT_SENSE=p[1]))


print sense_rdd.take(5)
sense_rdd.toDF().write\
  .option("sep",sep).option("header","true")\
  .csv("sense")


#Compute Overall Seasonality

# Filter ones who dont have enough months on book.
monthOnBook_rdd = dwhContents.map(lambda p: (int(p[0]), [int(p[4])]))\
.reduceByKey(lambda a,b: a+b)\
.map(lambda p: (p[0], list(set(p[1]))))\
.map(lambda p: (p[0], mm2index_bc.value[min(p[1])]))\
.filter(lambda p: p[1] < 12)

print monthOnBook_rdd.take(5)


overall_seasonality_tmp = dwhContents.map(lambda p: (int(p[0]), (int(p[4]), float(p[3]))))
overall_seasonality_rdd = overall_seasonality_tmp.join(monthOnBook_rdd)\
.map(lambda p: ((p[0], p[1][0][0]), p[1][0][1]))\
.reduceByKey(lambda a,b: a+b)\
.map(lambda p: (p[0][0], p[0][1], p[1]))\
.map(format_month_to_array_monthly)\
.map(compute_seasonal_score)\
.map(lambda p: Row(**p))

# overall_seasonality_rdd = dwhContents.map(lambda p: ((int(p[0]), int(p[4])), float(p[3])))\
# .reduceByKey(lambda a,b: a+b)\
# .map(lambda p: (p[0][0], p[0][1], p[1]))\
# .map(format_month_to_array_monthly)\
# .map(compute_seasonal_score)\
# .map(lambda p: Row(**p))

print overall_seasonality_rdd.take(5)

overall_seasonality_rdd.toDF().write\
  .option("sep",sep).option("header","true")\
  .csv("overall_seasonal")

#exit()
# create Catgory to Int mapper

merchant_arr = dwhContents.map(lambda p: p[2]).distinct().collect()
merchant2index = dict([(v,i) for i,v in enumerate(sorted(merchant_arr))])
print merchant2index

merchant2index_bc = ctx.broadcast(merchant2index)
index2merchant_bc = ctx.broadcast(dict([(v,k) for k,v in merchant2index.iteritems()]))

txn_rdd = dwhContents.map(lambda p: (int(p[0]), int(p[1]), merchant2index_bc.value[p[2]] ,float(p[3]), int(p[4]))).cache()

print txn_rdd.take(5)

txn_agg_rdd = txn_rdd.map(lambda p: ((p[0],p[2],p[4]),p[3]))\
.reduceByKey(lambda a,b: a+b)\
.map(lambda p: (p[0][0], p[0][1], p[0][2], p[1]))\
.map(format_month_to_array_cat)\
.map(lambda p: ((p[0], p[1]), p[3]))\
.reduceByKey(lambda a,b: a+b)

txn_cat_score = txn_agg_rdd.map(compute_cateogry_score)\
.map(lambda p: Row(**p))
#.map(lambda p: (p[0][0], index2merchant_bc.value[p[0][1]], p[1][0], p[1][1], p[1][2], p[1][3], p[1][4]))\
#.map(lambda p: Row(ACC_KEY=p[0], CATEGORY=p[1], SPENDING_FLAG=p[2], PVAL=p[3], SPENDING_TYPE=p[4], SEASONAL=p[5], USAGE_RATIO=p[6]))

print txn_cat_score.take(5)

txn_cat_score.toDF().write\
  .option("sep",sep).option("header","true")\
  .csv("txn_cat_overall")

txn_cat_seasonal = txn_agg_rdd.map(lambda p: (p[0][0], (p[0][1], p[1])))\
.join(monthOnBook_rdd)\
.map(lambda p: ((p[0], p[1][0][0]), p[1][0][1]))\
.map(compute_cateogry_seasonaity)\
.map(lambda p: Row(**p))

print txn_cat_seasonal.take(5)

txn_cat_seasonal.toDF().write\
  .option("sep",sep).option("header","true")\
  .csv("txn_cat_seasonal")

exit()


ctx.stop()
