from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import concat_ws, col, lit
import time
import argparse
from utils import *


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', type=str, default='Join')

    return parser.parse_args()

def run_sql(args):
	spark = SparkSession.builder.appName(args.app).getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel("TRACE")

	log4jLogger = sc._jvm.org.apache.log4j
	logger = log4jLogger.LogManager.getLogger('Join')

	# read data
	logger.info('Begin reading data')
	input_dir = 'alluxio://{}:19998/home/ec2-user/data'.format(get_master())

	item_df = spark.createDataFrame(convert_lineitem(sc.textFile('{}/lineitem.tbl'.format(input_dir))))

	item_part_df = item_df.select(concat_ws("|", item_df['l_quantity'], item_df['l_extendedprice'], item_df['l_discount'], item_df['l_tax'], item_df['l_returnflag'], item_df['l_linestatus'], item_df['l_shipdate']))

	item_part_df.write.text('{}/lineitem_part.tbl'.format(input_dir), lineSep="|\n")

	
	logger.info('Finish writing data')



if __name__ == '__main__':
	args = get_args()
	run_sql(args)

