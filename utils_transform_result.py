import pandas as pd
import os
import numpy as np
import math

base_path = 'output'

cate_df = pd.read_csv(os.path.join(base_path,'spending_category.csv'), sep='|')
sense_df = pd.read_csv(os.path.join(base_path,'spending_sense.csv'), sep='|')

full_df = pd.merge(left=cate_df, right=sense_df, how='inner')

SENSE_MAP = {0:'DOWN', 1:'NORMAL', 2:'UP'}

PIGEON_FLAGs = []
PIGEON_TYPEs = []

for tup in full_df.itertuples():
    confid = tup[4]
    t_score = tup[3]

    if confid > 10.0:
        PIGEON_FLAGs.append('N')
    elif t_score < 0:
        PIGEON_FLAGs.append('D')
    elif t_score > 0:
        PIGEON_FLAGs.append('U')

    if t_score == -10:
        PIGEON_TYPEs.append(301)
    elif t_score == 10:
        PIGEON_TYPEs.append(201)
    elif t_score == -14:
        PIGEON_TYPEs.append(302)
    elif t_score == -20:
        PIGEON_TYPEs.append(303)
    elif t_score == 11:
        PIGEON_TYPEs.append(202)
    elif t_score == 14:
        PIGEON_TYPEs.append(203)
    else:
        PIGEON_TYPEs.append(100)

full_df['PIGEON_FLAG'] = pd.Series(PIGEON_FLAGs)
full_df['PIGEON_TYPE'] = pd.Series(PIGEON_TYPEs)
full_df['PIGEON_SENSE'] = full_df['SENSE'].map(lambda x: SENSE_MAP[x])

#print full_df.head(60)
full_df.to_csv(os.path.join(base_path, 'cat.csv'),\
 columns=['ACC_NUMBER', 'MERCHANT', 'PIGEON_FLAG', 'PIGEON_TYPE', 'PIGEON_SENSE'],\
 header=True, index=False, sep='|')


def transform_overall(row):
    acc = int(row[0])
    vals = row[1:]

    edited_vals = []

    for v in vals:
        if v > 1.5:
            edited_vals.append('H')
        elif v < -1.5:
            edited_vals.append('L')
        else:
            edited_vals.append('MED')

    return [acc] + edited_vals

overall_df = pd.read_csv(os.path.join(base_path,'spending_overall.csv'), sep='|')

overall_df = overall_df.apply(transform_overall, axis=1)

overall_df.to_csv(os.path.join(base_path, 'overall.csv'),\
 header=True, index=False, sep='|')