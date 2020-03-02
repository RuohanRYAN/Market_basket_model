from pyspark import SparkContext, SparkConf
import json
from operator import add
import sys
import copy
import math
import time
import csv

# def main(review_path,business_path,output_path):
def main(review_path,business_path,output_path):
	def filter_state(x):
		if(x["state"]=="NV"):
			return True
		else:
			return False
	sc = SparkContext(appName = "inf553")
	rdd_review = sc.textFile(review_path)
	rdd_review_dict = rdd_review.map(json.loads).map(lambda x:(x["business_id"],x["user_id"]))
	rdd_bus = sc.textFile(business_path)
	rdd_bus_dict = rdd_bus.map(json.loads).filter(filter_state).map(lambda x:(x["business_id"],1))
	#### get rid of duplication ####
	rdd_review_unique = rdd_review_dict.map(lambda x:(x,1)).reduceByKey(add).map(lambda x:x[0])
	####
	user_business = rdd_bus_dict.join(rdd_review_unique).map(lambda x:(x[1][1],x[0]))

	# check_dup = user_business.map(lambda x:(x,1)).reduceByKey(add).map(lambda x:x[0])

	# print(user_business.count())
	# print(check_dup.count())
	# user_business_pro = user_business.groupByKey().mapValues(list).map(lambda x: (x[0],sorted(x[1]))).filter(lambda x:len(x[1])>70)
	# print(user_business_pro.count())
	# check_dup = user_business.map(lambda x:(x,1)).reduceByKey(add).filter(lambda x:x[1]>1)
	# print(check_dup.collect())

	write(output_path,user_business.collect())
def write(output_path,user_business):
	with open(output_path,"w") as file:
		file.write("user_id,business_id\n")
		for line in user_business:
			file.write(str(line[0]) + "," + str(line[1])+"\n")

review_path = "review.json"
business_path = "business.json"
output_path = "user_business.csv"
main(review_path,business_path,output_path)
