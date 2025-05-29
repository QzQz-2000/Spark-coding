from pyspark import SparkConf, SparkContext
import json
import os

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    base_dir = os.path.dirname(os.path.abspath(__file__))  # 当前脚本所在目录
    file_path = os.path.join(base_dir, "../data/input/order.txt")

    # 读取数据文件
    file_rdd = sc.textFile(file_path)

    # 进行rdd数据的split 按照|符号进行, 得到一个个的json数据
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))

    # 通过Python 内置的json库, 完成json字符串到字典对象的转换
    dict_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))

    # 过滤数据，只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda line: line["areaName"] == "北京")

    # 组合北京和商品类型形成新的字符串
    category_rdd = beijing_rdd.map(lambda x: x["areaName"] + "_" + x["category"])

    # 对结果进行去重
    result_rdd = category_rdd.distinct()

    print(result_rdd.collect())
    # ['北京_平板电脑', '北京_家电', '北京_电脑', '北京_家具', '北京_书籍', '北京_食品', '北京_服饰', '北京_手机']