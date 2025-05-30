from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    def process(iter):
        result = list()
        for it in iter:
            result.append(it*10)
        print(result)

    rdd1.foreachPartition(process)