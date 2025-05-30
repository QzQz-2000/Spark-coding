from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/input/words.txt")

    rdd2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))

    result = rdd2.countByKey()

    print(result)
    # defaultdict(<class 'int'>, {'hadoop': 3, 'spark': 4, 'kafka': 1})  
