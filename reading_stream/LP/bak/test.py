import time
from multiprocessing import Pool


def f(x):
    return x*x

with Pool(processes=4) as p:

    start = time.time() # 開始測量執行時間

    # 設定 chunksize 為 1
    result1 = p.map(f, range(1000000), chunksize=1)

    end = time.time() # 結束測量執行時間
    print("chunksize 為 1，執行時間為 %f 秒" % (end - start))


    start = time.time() # 開始測量執行時間

    # 設定 chunksize 為 1000
    result2 = p.map(f, range(1000000), chunksize=1000)

    end = time.time() # 結束測量執行時間
    print("chunksize 為 1000，執行時間為 %f 秒" % (end - start))

    if result1 == result2:
        print("結果相同")
