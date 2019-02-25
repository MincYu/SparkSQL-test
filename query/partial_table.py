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
    # 0: join; 1: aggr; 2:filter
    parser.add_argument('--query', type=int, default=0)
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

	order_df = spark.createDataFrame(convert_orders(sc.textFile('{}/orders.tbl'.format(input_dir))))
	item_df = spark.createDataFrame(convert_lineitem(sc.textFile('{}/lineitem.tbl'.format(input_dir))))

	item_part_df = spark.createDataFrame(convert_part_lineitem(sc.textFile('{}/lineitem_part.tbl'.format(input_dir))))

	logger.info('Finish reading data')

	# create views
	order_df.createOrReplaceTempView('orders')
	item_df.createOrReplaceTempView('lineitem')
	item_part_df.createOrReplaceTempView('lineitem_part')

	query_list = [join_tables, aggregation, filter_lineitem, Q1_0, Q1_1]

	query = query_list[args.query]()

	# query
	result_df = spark.sql(query)

	logger.info('Begin executing query')
	begin_time = now()

	result_df.show()

	exe_time = gap_time(begin_time)
	logger.info('End executing query. Time: {}'.format(exe_time))

	spark.catalog.clearCache()

def join_tables():
	return "select * from ORDERS, LINEITEM where l_orderkey = o_orderkey"

def aggregation():
	return "select l_shipmode, count(l_shipmode) from LINEITEM group by l_shipmode"

def filter_lineitem():
	return "select * from LINEITEM where l_discount >= 0.05 and l_discount <= 0.1"

def Q1_0():
	return """
		SELECT l_returnflag, l_linestatus,
			sum(l_quantity) as sum_qty,
			sum(l_extendedprice) as sum_base_price,
			sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
			sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
			avg(l_quantity) as avg_qty,
			avg(l_extendedprice) as avg_price,
			avg(l_discount) as avg_disc,
			count(*) as count_order
		FROM lineitem_part
		WHERE l_shipdate <= "1998-09-02"
		GROUP BY l_returnflag, l_linestatus
		ORDER BY l_returnflag, l_linestatus
	"""

def Q1_1():
	return """
		SELECT l_returnflag, l_linestatus,
			sum(l_quantity) as sum_qty,
			sum(l_extendedprice) as sum_base_price,
			sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
			sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
			avg(l_quantity) as avg_qty,
			avg(l_extendedprice) as avg_price,
			avg(l_discount) as avg_disc,
			count(*) as count_order
		FROM lineitem
		WHERE l_shipdate <= "1998-09-02"
		GROUP BY l_returnflag, l_linestatus
		ORDER BY l_returnflag, l_linestatus
	"""

if __name__ == '__main__':
	args = get_args()
	run_sql(args)

