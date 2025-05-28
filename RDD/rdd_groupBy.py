from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('a', 1), ('b', 1)])
    # groupBy传入函数的意思是，通过这个函数，确定按照谁来分组，这里我们按照第一个元素来分组
    # 分组规则和sql是一致的
    result = rdd.groupBy(lambda t: t[0])

    print(result.map(lambda t: (t[0], list(t[1]))).collect())

    # result: [('b', [('b', 1)]), ('a', [('a', 1), ('a', 1)])] 