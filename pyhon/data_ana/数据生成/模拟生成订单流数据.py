import random
import time
import datetime
# 模拟生成订单流数据, 数据有时间戳, 用户ID,产品ID,价格, 数量。保存到字典中。其中用户ID为固定长度为10的字符串
# 产品ID股东长度为5
def gen_order():
    order = {}
    order['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    order['user_id'] = ''.join(random.sample('0123456789', 10))
    order['product_id'] = ''.join(random.sample('0123456789', 5))
    order['price'] = random.randint(1, 100)
    order['quantity'] = random.randint(1, 10)
    return order
#每秒钟生成5-20个订单里流数据,最多生成1000次
def gen_orders():
    for i in range(1000):
        for j in range(random.randint(5, 20)):
            yield gen_order()
        time.sleep(1)

if __name__ == '__main__':
    for order in gen_orders():
        print(order)