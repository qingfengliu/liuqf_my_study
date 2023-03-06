# 随机生成一串数据.列名为 insurant_cid_number,splancode,applirelation_deal,insurant_sex,
# insurant_birthday_cid,acceptdate,policystatus,pay_source,cid_number_md5,member_id
# 要求insurant_cid_number长度为40的数字和字母,不能重复
# 要求splancode首个字符必须为字母,后边的字符必须为数字,长度为10
# 要求applirelation_deal为1,2,3,4,5,6,7,8,9,10
# 要求insurant_sex为0,1,NULL
# 要求insurant_birthday_cid为时间戳,范围为1992-01-01到2000-01-01或者为NULL
# 要求acceptdate为时间戳,范围为2020-01-01到2023-01-01
# 要求policystatus为0,1,2,3,NULL
# 要求pay_source为0,1,2,3,NULL
# 要求cid_number_md5为insurant_cid_number的md5值
# 要求member_id为长度为10的数字并转换为字符串

import random
import datetime
import string
import hashlib
import pandas as pd

def gen_insurant():
    insurant = {}
    insurant['insurant_cid_number'] = ''.join(random.sample(string.ascii_letters + string.digits, 40))
    insurant['splancode'] = ''.join(random.sample(string.ascii_letters, 1)) + ''.join(random.sample(string.digits, 9))
    insurant['applirelation_deal'] = str(random.randint(1, 10))
    insurant['insurant_sex'] = random.choice(['0', '1', None])
    insurant['insurant_birthday_cid'] = str(random.randint(631152000, 946656000))
    insurant['acceptdate'] = str(random.randint(1577836800, 1640995200))
    # insurant['insurant_birthday_cid'] = datetime.datetime.fromtimestamp(random.randint(631152000, 946656000)).strftime('%Y-%m-%d %H:%M:%S')
    # insurant['acceptdate'] = datetime.datetime.fromtimestamp(random.randint(1577836800, 1640995200)).strftime('%Y-%m-%d %H:%M:%S')
    insurant['policystatus'] = random.choice(['0', '1', '2', '3', None])
    insurant['pay_source'] = random.choice(['0', '1', '2', '3', None])
    insurant['cid_number_md5'] = hashlib.md5(insurant['insurant_cid_number'].encode('utf-8')).hexdigest()
    insurant['member_id'] = str(random.randint(1000000000, 9999999999))
    return insurant

#循环生成6000条
def gen_insurants():
    for i in range(8000):
        yield gen_insurant()

#将gen_insurants生成的数据保存pd.DataFrame中然后保存成csv

df = pd.DataFrame(gen_insurants())

# 随机将df中的insurant_birthday_cid列赋值为空
df['insurant_birthday_cid'] = df['insurant_birthday_cid'].apply(lambda x: None if random.randint(1, 10) == 1 else x)
# df 添加一列age,如果insurant_birthday_cid为空,则age为时间戳,范围为2000-01-01到2010-01-01否则为insurant_birthday_cid
df['age'] = df['insurant_birthday_cid'].apply(lambda x: random.randint(946656000, 1262304000) if x is None else x)

# df 添加一列member_gender,如果insurant_sex为空,则member_gender随机为0或者1.否则为insurant_sex
df['member_gender']=df['insurant_sex'].apply(lambda x: random.choice(['0', '1', None]) if x is None else x)

df['member_cidnumber']=df['insurant_cid_number']

dingdan=df[['insurant_cid_number','splancode',
            'applirelation_deal','insurant_sex','insurant_birthday_cid','acceptdate',
            'policystatus','pay_source','cid_number_md5']].copy()

dingdan.to_csv(r'D:\订单数据.csv', index=False)
member=df[['member_id','member_cidnumber','age','member_gender']].copy()

#member 根据member_id去重,并且取第一条
member=member.drop_duplicates(subset=['member_id'],keep='first')
member.to_csv(r'D:\会员数据.csv', index=False)


#将 df 中的splancode取出组成一个新的dataframe
splaninfo=df[['splancode']].copy()
#去重
splaninfo=splaninfo.drop_duplicates(subset=['splancode'],keep='first')
#将splaninfo reset_index,然后将index作为新的一列
splaninfo=splaninfo.reset_index()[['splancode']].reset_index()
#将splaninfo的index列转换为字符串并且加上前缀系列
splaninfo['clause_type']=splaninfo['index'].apply(lambda x:'系列'+str(x//10+1))
splaninfo=splaninfo[['splancode','clause_type']]

splaninfo.to_csv(r'D:\产品数据.csv', index=False)

