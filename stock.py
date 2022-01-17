# -*- coding: utf-8 -*-
import sys
import jpstocks
import warnings
import pandas as pd
import datetime as dt
import time as time
from sqlalchemy import create_engine

warnings.simplefilter("ignore", category=UserWarning)
q = jpstocks.Quotes()
categories = [
 '0050',  # 農林・水産業
 '1050',  # 鉱業
 '2050',  # 建設業
 '3050',  # 食料品
 '3100',  # 繊維製品
 '3150',  # パルプ・紙
 '3200',  # 化学
 '3250',  # 医薬品
 '3300',  # 石油・石炭製品
 '3350',  # ゴム製品
 '3400',  # ガラス・土石製品
 '3450',  # 鉄鋼
 '3500',  # 非鉄金属
 '3550',  # 金属製品
 '3600',  # 機械
 '3650',  # 電気機器
 '3700',  # 輸送機器
 '3750',  # 精密機器
 '3800',  # その他製品
 '4050',  # 電気・ガス業
 '5050',  # 陸運業
 '5100',  # 海運業
 '5150',  # 空運業
 '5200',  # 倉庫・運輸関連業
 '5250',  # 情報・通信
 '6050',  # 卸売業
 '6100',  # 小売業
 '7050',  # 銀行業
 '7100',  # 証券業
 '7150',  # 保険業
 '7200',  # その他金融業
 '8050',  # 不動産業
 '9050'   # サービス業
]
for i in range(len(categories)):
    try:
        brands = q.get_brand(categories[i])
    except:
        pass
    lis = []
    for b in brands:
        f = q.get_finance(b.ccode)
        # start = dt.date.today()
        start = dt.datetime(2020, 2, 7)
        # end = dt.date.today()
        end = dt.datetime(2020, 2, 7)
        try:
            h = q.get_historical_prices(b.ccode, jpstocks.DAILY, start_date=start, end_date=end)
        except jpstocks.exceptions.CCODENotFoundException:
            pass
        try:
            yesterdayPrice = h.__getitem__(0).close
            if ((f.years_low + f.years_high) / 2 - yesterdayPrice) / ((f.years_low + f.years_high) / 2) > 0:  # 低于平均价（平均价-现在价 / 平均价）
                dic = {
                    'category_code': categories[i],
                    'market_name': b.market,
                    'stock_code': b.ccode,
                    'company_name': b.name,
                    'market_cap': f.market_cap / 100,  # 亿日元
                    'years_low_price': f.years_low,
                    'years_high_price': f.years_high,
                    'years_average_price': (f.years_low + f.years_high) / 2,
                    'yesterday_price': yesterdayPrice,
                    'diff_average_price': yesterdayPrice - (f.years_low + f.years_high) / 2,  # 平均差价
                    'decline_ratio': '%.2f' % ((((f.years_low + f.years_high) / 2 - yesterdayPrice) / ((f.years_low + f.years_high) / 2)) * 100),  # 平均价跌幅比率
                    'dividend_one': f.dividend_one,
                    'price_min': '%.2f' %(f.price_min / 10000),  # 万日元
                    'create_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                }
                lis.append(dic)
        except:
            pass
    engine = create_engine(
        'mysql+pymysql://aliyun:123456@47.74.5.159:3306/blog')
    sql = "delete from t_stock_info where create_time < " + "'" + dt.date.today().__str__() + "'"
    try:
        pd.read_sql_query(sql, engine)
    except:
        print('deleted')
    df = pd.DataFrame(lis)
    table_name = "t_stock_info"
    df.to_sql(table_name, engine, schema='blog', if_exists='append', index=False)
