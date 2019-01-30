from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import argparse
from utils import *


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', type=str, default='Join')
    return parser.parse_args()

def join_two_tables(args):
	spark = SparkSession.builder.appName(args.app).getOrCreate()
	sc = spark.sparkContext

	log4jLogger = sc._jvm.org.apache.log4j
	logger = log4jLogger.LogManager.getLogger('Join')

	# read data
	logger.info('Begin reading data')
	input_dir = 'alluxio://{}:19998/tpch'.format(get_master())

	order_df = spark.createDataFrame(convert_orders(sc.textFile('{}/orders.tbl'.format(input_dir))))
	item_df = spark.createDataFrame(convert_lineitem(sc.textFile('{}/lineitem.tbl'.format(input_dir))))

	logger.info('Finish reading data')

	# create views
	order_df.createOrReplaceTempView('orders')
	item_df.createOrReplaceTempView('lineitem')

	# query
	result_df = spark.sql(
		''' 
		select * from ORDERS, LINEITEM where l_orderkey = o_orderkey
		'''
		)

	logger.info('Begin executing query')
	begin_time = now()

	result_df.show()

	exe_time = gap_time(begin_time)
	logger.info('End executing query. Time: {}'.format(exe_time))

	spark.catalog.clearCache()


if __name__ == '__main__':
	args = get_args()
	join_two_tables(args)