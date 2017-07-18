# PIGEON

1. Install Python packages

(sudo) pip install -r requirements.txt

2. Edit config.ini

START_DATE: 2014-01 <-- The leastest month of transaction data

END_DATE: 2017-04 <-- The most recent month of transaction data

INPUT_DIR: ./data/behaviour_txn_latest.csv <-- path to input file

OUTPUT_DIR: ./output <- output directory

LOGGING_DIR: ./logs <- logging directory

n, p, s can leave it as it is.

3. Run the program

python PIGEON.py -c config.ini

===============================

The result will be saved into OUTPUT_DIR.
