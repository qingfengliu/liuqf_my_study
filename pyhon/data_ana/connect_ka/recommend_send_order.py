import csv
import time

from kafka3 import KafkaProducer

producer = KafkaProducer(bootstrap_servers='liuqf1:9092')

with open('D:/泰康学习/造数/订单数据.csv', 'r', newline='',encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)  # 创建csv.reader对象
    # 使用send方法发送消息,每秒发送十条
    for i,row in enumerate(reader):
        producer.send('recommendmember', str(row).encode('utf-8'))
        if i % 10 == 0:
            time.sleep(1)

