from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(101, "zhangsan"), (102, "zhaosi"), (103, "wangwu")])

    rdd2 = sc.parallelize([(101, "销售部"), (102, "科技部")])

    rdd3 = rdd1.join(rdd2)

    print(rdd3.collect())
    # [(101, ('zhangsan', '销售部')), (102, ('zhaosi', '科技部'))]   

    print(rdd1.leftOuterJoin(rdd2).collect())
    # [(101, ('zhangsan', '销售部')), (102, ('zhaosi', '科技部')), (103, ('wangwu', None))]

    print(rdd1.rightOuterJoin(rdd2).collect())
    # [(101, ('zhangsan', '销售部')), (102, ('zhaosi', '科技部'))]