import re
data='投诉描述：妈的，一天瞎搞，马上给我退回来  投诉时间：2022-11-07T10:51:04  交易时间：2022-11-07 10:34:10  交易金额：76.50  商户订单号：TKCNUPP1001721345166262  微信订单号：42000678405249587'

#使用正则表达式从data字符串中提取出投诉时间、交易时间、交易金额,如果匹配失败返回空值
#投诉时间：2022-11-07T10:51:04
#交易时间：2022-11-07 10:34:10
#交易金额：76.50
def get_time(data):
    #匹配投诉时间
    time1 = re.search(r'投诉时间：(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', data)
    if time1:
        time1 = time1.group(1)
    else:
        time1 = ''
    #匹配交易时间
    time2 = re.search(r'交易时间：(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', data)
    if time2:
        time2 = time2.group(1)
    else:
        time2 = ''
    #匹配交易金额
    money = re.search(r'交易金额：(\d+\.\d+)', data)
    if money:
        money = money.group(1)
    else:
        money = ''
    return time1, time2, money

#使用正则表达式从data字符串中提取出投诉描述,如果匹配失败返回空值
#投诉描述：妈的，一天瞎搞，马上给我退回来
def get_desc(data):
    desc = re.search(r'投诉描述：(.*)\s+投诉时间', data)
    if desc:
        desc = desc.group(1)
    else:
        desc = ''
    return desc
