from kafka import KafkaProducer

#通过KafkaProducer创建一个生产者对象，bootstrap_servers指定kafka集群地址
producer = KafkaProducer(bootstrap_servers='liuqf:9092')
#向first主题发送消息
producer.send('first', b'hello kafka')
producer.close()