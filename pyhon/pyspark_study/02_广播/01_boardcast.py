from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test01")

    sc = SparkContext(conf=conf)
    stu_info_list=[(1,'张大仙',11),
                   (2,'王晓晓',13),
                   (3,'张甜甜',11),
                   (4,'王大力',11)]
    #编辑为广播变量
    boardcast=sc.broadcast(stu_info_list)
    score_info_rdd=sc.parallelize([
        (1,'语文',99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
    ])
    def map_func(data):
        id=data[0]
        name=''
        for stu_info in boardcast.value:
            stu_id=stu_info[0]
            if id==stu_id:
                name = stu_info[1]
        return (name,data[1],data[2])
    print(score_info_rdd.map(map_func).collect())
    sc.stop()