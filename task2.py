from pyspark import SparkContext, SparkConf
import json
from operator import add
import sys
import copy
import math
import time
from itertools import combinations

def main(K,support,input_path,output_path):
	rdd = readfile(input_path).repartition(25).persist()
	# rdd = readfile(input_path)
	# print(rdd.getNumPartitions())
	p = 1/rdd.getNumPartitions()
	s = math.floor(p*support)
	def A_prior(input):
		def epoch(content,prior,num,s):
			if(num>len(prior)):return False
			pos_prior = {};toggle = False
			for List in content:
				b_id = List[1];array = []
				if(num>len(b_id)):continue
				toggle = True
				for e in b_id:
					if(prior.get(e,0)==1): array.append(e)
				print(len(b_id))
				print(len(array))
				print("****************")
				for element in combinations(array,num):
					pos_prior[element] = pos_prior.get(element,0)+1
			for key in pos_prior.keys():
				pos_prior[key] = 1 if pos_prior[key]>=s else 0
			if(toggle):
				return pos_prior
			else:
				return toggle
		#################################
		single_map = {}
		content = []
		prior = {}
		result = []
		for x in input:
			content.append(x)
		########################################################

		for List in content:
			b_id = List[1]
			for i in range(len(b_id)):
				prior[b_id[i]] = prior.get(b_id[i],0)+1
		for key in prior.keys():
			if(prior[key]>=s):
				prior[key] = 1
				# print(key)
				# result.append("("+str(key)+")")
				result.append(tuple([str(key)]))
			else:
				prior[key] = 0
		i = 2
		while(True):
			pos_prior = epoch(content,prior,i,s)
			if(pos_prior==False): break
			temp = []
			for key in pos_prior.keys():
				if(pos_prior[key]==1):
					for child in key:
						temp.append(child)
					# result.append(str(key))
					result.append(key)
			if(len(temp)!=0):
				prior = {}
				sorted_temp = sorted(set(temp))
				for element in sorted_temp:
					prior[element] = 1
			else:
				prior = {}
			i+=1	
			# print(result)
			for k in result:
				yield(k,1)

	def A_prior_improved(input):
		def epoch(content,prior,num,s,pair_map,pre_pos_prior):
			def check_subset(element,num,pre_pos_prior):
				for e in combinations(element,num):
					if(num==1):
						e = e[0]
					if(pre_pos_prior.get(e,0)==1):
						continue
					else:
						return False
				return True
			if(num>len(prior)):return False
			pos_prior = {};toggle = False
			for List in content:
				b_id = List[1];array = []
				if(num>len(b_id)):continue
				toggle = True
				for e in b_id:
					if(prior.get(e,0)==1): array.append(e)
				print(len(b_id))
				print(len(array))
				print(num)
				print("***********************")
				for element in combinations(array,num):
					if(num==2 and pair_map.get(hash_fun(element,n_buckets,String=True),0)==0):
						continue
					if(check_subset(element,num-1,pre_pos_prior)):
						pos_prior[element] = pos_prior.get(element,0)+1
					# pos_prior[element] = pos_prior.get(element,0)+1
			for key in pos_prior.keys():
				pos_prior[key] = 1 if pos_prior[key]>=s else 0
			if(toggle):
				return pos_prior
			else:
				return toggle
		#################################
		single_map = {}
		pair_map = {}
		content = []
		prior = {}
		result = []
		n_buckets = 10000
		for x in input:
			content.append(x)
		########################################################

		for List in content:
			b_id = List[1]
			for i in range(len(b_id)):
				prior[b_id[i]] = prior.get(b_id[i],0)+1
				for j in range(i+1,len(b_id)):
					index = hash_fun([b_id[i],b_id[j]],n_buckets,String=True)
					pair_map[index] = pair_map.get(index,0)+1

		for key in prior.keys():#### generate bit map for singleton ###
			if(prior[key]>=s):
				prior[key] = 1
				result.append(tuple([str(key)]))
			else:
				prior[key] = 0
		for key in pair_map.keys():
			if(pair_map[key]>=s):
				pair_map[key] = 1
			else:
				pair_map[key] = 0
		i = 2
		pre_pos_prior = prior
		while(True):
			pos_prior = epoch(content,prior,i,s,pair_map,pre_pos_prior)
			pre_pos_prior = copy.deepcopy(pos_prior)
			if(pos_prior==False): break
			temp = []
			for key in pos_prior.keys():
				if(pos_prior[key]==1):
					for child in key:
						temp.append(child)
					# result.append(str(key))
					result.append(key)
			if(len(temp)!=0):
				prior = {}
				sorted_temp = sorted(set(temp))
				for element in sorted_temp:
					prior[element] = 1
			else:
				prior = {}
			i+=1	
			# print(result)
			for k in result:
				yield(k,1)	
	def f(x):
		return x!="user_id,business_id"
	def split_fun(x):
		a = x.split(',')
		return a 
	def PCY(input):
		content = []
		for x in input:
			content.append(x)
		single_map = {}
		pair_map = {}
		trip_map = {}
		n_buckets = 10000
		n_buckets_trip = 10000
		final_map = {}
		final_sub_seq = []
		### first path ####
		for List in content:
			b_id = List[1]
			for i in range(len(b_id)):
				single_map[b_id[i]] = single_map.get(b_id[i],0)+1
				for j in range(i+1,len(b_id)):
					index = hash_fun([b_id[i],b_id[j]],n_buckets,String=True)
					pair_map[index] = pair_map.get(index,0)+1
					# for k in range(j+1, len(b_id)):
					# 	index_trip = hash_fun([b_id[i],b_id[j],b_id[k]],n_buckets_trip,String = True)
					# 	trip_map[index_trip] = trip_map.get(index_trip,0)+1
		for key in single_map.keys():#### generate bit map for single ####
			if(single_map[key]>=s):
				single_map[key] = 1
			else:
				single_map[key] = 0
		for key in pair_map.keys(): ### generate bit map for pair ###
			if(pair_map[key]>=s):
				pair_map[key] = 1
			else:
				pair_map[key] = 0
		# for key in trip_map.keys():### generate bit map for trip sets ####
		# 	if(trip_map[key]>=s):
		# 		trip_map[key] = 1
		# 	else:
		# 		trip_map[key] = 0
		######## second path #####
		# inverse_map = {}
		# for List in content:
		# 	b_id = List[1]
		# 	temp = []
		# 	result = []
		# 	array = []
		# 	for element in b_id:
		# 		if(single_map[element]==1):
		# 			array.append(element)
		# 	print(len(b_id))
		# 	print(len(array))
		# 	getSubset(sorted(array),temp,result,0,-1,pair_map,n_buckets)
		# 	print("********************")
		# 	# print(len(result))
		# 	for sub_seq in result:
		# 		key = concat(sub_seq)
		# 		inverse_map[key] = sub_seq
		# 		final_map[key] = final_map.get(key,0)+1
		###########
		# inverse_map = {}
		for List in content:
			array = []
			b_id = List[1]
			for element in b_id:
				if(single_map[element]==1):
					array.append(element)
			print(len(b_id))
			print(len(array))
			print("*******************")
			for i in range(1,len(array)+1):
				for element in combinations(array,i):
					key = concat(element)
					# inverse_map[key] = element
					if(i==2):
						if(pair_map[hash_fun(element,n_buckets,String=True)]==1):
							final_map[key] = final_map.get(key,0)+1
					# elif(i==3):
					# 	if(trip_map[hash_fun(element,n_buckets_trip,String=True)]==1):
					# 		final_map[key] = final_map.get(key,0)+1
					else:
						final_map[key] = final_map.get(key,0)+1
		for key in final_map.keys():
			if(final_map[key]>=s):
				# final_sub_seq.append(inverse_map[key])
				final_sub_seq.append(key)
		for i in final_sub_seq:
			yield (i,1)
	def sort(x):
		a = list(x)
		a.sort()
		return tuple(a)
	rdd_cleaned = rdd.filter(f).map(split_fun).groupByKey().mapValues(list).map(lambda x: (x[0],sorted(x[1]))).filter(lambda x:len(x[1])>K)
	rdd_cleaned_A_priori = rdd_cleaned.mapPartitions(A_prior_improved).reduceByKey(add)
	candidate = rdd_cleaned_A_priori.map(lambda x:(len(x[0]),x[0])).groupByKey().sortBy(lambda x:x[0]).mapValues(sort)
	candit = rdd_cleaned_A_priori.map(lambda x:x[0])
	###################### check candidate pairs ######################
	# bit_map = {}
	# length_array = []
	# for c in candit.collect():
	# 	bit_map[c]=1
	# 	length_array.append(len(c))
	# length_array = set(length_array)
	# def filter_sets(x):
	# 	input = x[1]
	# 	result = []
	# 	for length in length_array:
	# 		for element in combinations(input,length):
	# 			if(bit_map.get(element,0)==1): result.append((element,1))
	# 	return result 
	# def filter_fun(x):
	# 	return x[1]>=support
	# frequent_set = rdd_cleaned.map(filter_sets).flatMap(lambda x:x).reduceByKey(add).filter(filter_fun).map(lambda x:(len(x[0]),x[0])).groupByKey().sortBy(lambda x:x[0]).mapValues(sort)
	# write(output_path,candidate.collect(),frequent_set.collect())
	def filter_fun(x):
		return x[1]>=support
	candit = candit.collect()
	def filter_sets(x):
		result = []
		input = x[1]
		for can in candit:
			if(set(can).issubset(input)):
				result.append((can,1))
		return result
	frequent_set = rdd_cleaned.map(filter_sets).flatMap(lambda x:x).reduceByKey(add).filter(filter_fun).map(lambda x:(len(x[0]),x[0])).groupByKey().sortBy(lambda x:x[0]).mapValues(sort)
	write(output_path,candidate.collect(),frequent_set.collect())

def write(output_path,candidate,frequent_set):
	with open(output_path,"w") as file:
		file.write("Candidates:"+"\n")
		for element in candidate:
			length = element[0]
			content = element[1]
			for i in range(len(content)):
				if(length==1):
					value = "('"+str(content[i][0])+"')"
				else:
					value = str(content[i])
				if(i!=len(content)-1):
					file.write(value+",")
				else:
					file.write(value+"\n")
			file.write("\n")

		file.write("Frequent Itemsets:\n")
		for element in frequent_set:
			length = element[0]
			content = element[1]
			for i in range(len(content)):
				if(length==1):
					value = "('"+str(content[i][0])+"')"
				else:
					value = str(content[i])
				if(i!=len(content)-1):
					file.write(value+",")
				else:
					file.write(value+"\n")
			file.write("\n")
def readfile(input_path):
	sc = SparkContext(appName = "inf553")
	# sc.setLogLevel("ERROR")
	return sc.textFile(input_path)
def concat(array):
	result = ""
	for a in array:
		result+=a
	return result
def hash_fun(array,n_buckets,String=False):
	if(not String):
		num = 0
	else:
		num = ""
	for a in array:
		num+=a

	return hash(num)%n_buckets
def getSubset_all(input,temp,result,ind,bit_map):
	if(ind==len(input)):
		if(bit_map.get(tuple(temp),0)==1 and len(temp)!=0):
			result.append((tuple(copy.deepcopy(temp)),1))
		return;
	temp.append(input[ind])
	getSubset_all(input,temp,result,ind+1,bit_map)
	temp.pop()
	getSubset_all(input,temp,result,ind+1,bit_map)
def getSubset(input,temp,result,ind,level,bit_map,n_buckets):
	if(ind==len(input)):
		if(len(temp)==2 and bit_map[hash_fun(temp,n_buckets,String=True)]!=1):
			return;
		if(len(temp)>level and len(temp)!=0):
			return;
			result.append(tuple(copy.deepcopy(temp)))
		return;
	temp.append(input[ind])
	getSubset(input,temp,result,ind+1,level,bit_map,n_buckets)
	temp.pop()
	getSubset(input,temp,result,ind+1,level,bit_map,n_buckets)
K = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]

start = time.time()
main(K,support,input_path,output_path)
end = time.time()
print("Duration: " + str(round(end-start)))