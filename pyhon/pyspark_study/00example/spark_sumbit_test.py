from pyspark.sql import SparkSession
import os
import socket

if __name__=='__main__':
    spark = SparkSession.builder.master("spark://192.168.42.128:7077").getOrCreate()
    #获取本机电脑名
    myname = socket.getfqdn(socket.gethostname())
    #获取本机ip
    myaddr = socket.gethostbyname(myname)
    print(myname)
    print(myaddr)

    spark.stop()