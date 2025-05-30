from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    print(rdd1.repartition(1).getNumPartitions())
    # 1
    print(rdd1.repartition(5).getNumPartitions())
    # 5
    print(rdd1.coalesce(5).getNumPartitions())
    # 3
    print(rdd1.coalesce(5, shuffle=True).getNumPartitions())
    # 5