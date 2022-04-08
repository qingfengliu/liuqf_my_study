import jieba

if __name__ == '__main__':
    content="小明硕士毕业于中国科学院计算机所,后在清华大学深造"
    result=jieba.cut(content,True)
    print(list(result))

    result2=jieba.cut(content,False)
    print(list(result2))

    #搜索引擎模式.等同于cu第二个参数为True.允许二次组合
    result3=jieba.cut_for_search(content)
    print(",".join(result3))