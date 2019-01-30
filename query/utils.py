from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

now = lambda: time.time()
gap_time = lambda past_time : int((now() - past_time) * 1000)

splitter = lambda l: l.split('|')

convert_orders = lambda rdd: rdd.map(splitter).map(lambda p: Row(o_orderkey=p[0], o_custkey=p[1], o_orderstatus=p[2], o_totalprice=p[3], \
	o_orderdate=p[4], o_orderpriority=p[5], o_clerk=p[6], o_shippriority=p[7], o_comment=p[8]))

convert_lineitem = lambda rdd: rdd.map(splitter).map(lambda p: Row(l_orderkey=p[0], l_partkey=p[1], l_suppkey=p[2], l_linenumber=p[3], \
	l_quantity=p[4], l_extendedprice=p[5], l_discount=p[6], l_tax=p[7], l_returnflag=p[8], l_linestatus=p[9], l_shipdate=p[10], \
	l_commitdate=p[11], l_receiptdate=p[12], l_shipinstruct=p[13], l_shipmode=p[14], l_comment=p[15]))

def get_master():
	with open('/home/ec2-user/hadoop/conf/masters', 'r') as f:
		return f.readline().rstrip()