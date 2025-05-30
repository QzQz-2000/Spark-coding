from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([('a', 1), ('E', 9), ('c', 1), ('F', 6), ('b', 1), ('G', 5)])

    # 按照自定义的函数进行分区
    def process(key):
        if key == 'a' or key == 'b':
            return 0
        if key == 'E':
            return 1
        return 2

    print(rdd1.partitionBy(3, process).glom().collect())
    # [[('a', 1), ('b', 1)], [('E', 9)], [('c', 1), ('F', 6), ('G', 5)]]