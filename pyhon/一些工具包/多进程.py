#1.使用进程池
from multiprocessing import Process,Manager,Pool



def hanshu(i, ceshi):
    ceshi['编号'+str(i)]=i

if __name__ == '__main__':

    jobs=[]
    manager = Manager()
    return_dict = manager.dict()
    pool=Pool(10)  #进程池,如果池中进程满了会阻塞知道有进程位置

    for i in range(0,100):
        p=pool.apply_async(hanshu,(i,return_dict))
        jobs.append(p)

    pool.close()
    pool.join()
    for k,v in return_dict.items():
        print(k,v)