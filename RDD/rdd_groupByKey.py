from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('a', 1), ('b', 1)])
    rdd2 = rdd1.groupByKey()

    # 要想查看结果，需要进行map
    print(rdd2.collect())
    # [('b', <pyspark.resultiterable.ResultIterable object at 0x1164d2900>), ('a', <pyspark.resultiterable.ResultIterable object at 0x138c56e90>)]
    
    # 只保留value
    print(rdd2.map(lambda x: (x[0], list(x[1]))).collect())
    # [('b', [1]), ('a', [1, 1])]