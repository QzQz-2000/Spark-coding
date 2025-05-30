from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 3, 4, 1, 4])

    print(rdd.takeSample(True, 20))
    # [4, 4, 4, 4, 1, 4, 3, 3, 5, 1, 2, 1, 5, 2, 2, 5, 4, 4, 1, 1]  
    