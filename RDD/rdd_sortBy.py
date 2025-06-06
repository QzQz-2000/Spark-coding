from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('a', 9), ('c', 1), ('c', 6), ('b', 1), ('b', 5)])

    # 如果要全局有序，排序分区数应该为1
    rdd2 = rdd1.sortBy(lambda x: x[1], ascending=True, numPartitions=3)

    print(rdd2.collect())
    # [('a', 1), ('c', 1), ('b', 1), ('b', 5), ('c', 6), ('a', 9)] 
