from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1)])
    rdd2 = sc.parallelize([('a', 1), ('c', 1), ('b', 1)])

    print(rdd1.intersection(rdd2).collect())
    # [('a', 1), ('b', 1)] 