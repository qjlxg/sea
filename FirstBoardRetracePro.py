import pandas as pd
import numpy as np
import os
import glob
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

# 战法名称：首板缩量回踩底部战法 (Pro版)
# 逻辑说明：
# 1. 寻找10日内首个涨停板（主板10%）。
# 2. 随后的回调天数 <= 7天，且成交量逐级缩减（主力洗盘标志）。
# 3. 价格回踩至涨停柱底部区域（支撑位）。
# 4. 结合历史类似形态回测胜率进行买入强度定级。

def analyze_stock(file_path, stock_names_map):
    try:
        df = pd.read_csv(file_path)
        # 统一列名以防编码或格式问题
        df.columns = ['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'pct_chg', 'change', 'turnover']
        df = df.sort_values('date')
        
        if len(df) < 15: return None
        
# 3. 统计回调特征 (从涨停次日到今天)
        retrace_df = recent_df.iloc[limit_up_idx+1:]
        retrace_days = len(retrace_df)
        
        # 实战参数：回调 2-5 天最佳
        if not (2 <= retrace_days <= 5): return None
        
        # 4. 关键硬指标过滤
        limit_bar = recent_df.iloc[limit_up_idx]
        
        # 指标A：回调期间严禁出现大阴线 (跌幅 > 7%)
        if (retrace_df['pct_chg'] < -7.0).any(): return None
        
        # 指标B：成交量萎缩 (今日量必须小于涨停日量的 50%)
        v_ratio = last_bar['volume'] / limit_bar['volume']
        if v_ratio > 0.7: return None
        
        # 指标C：价格回踩区间 [开盘价 * 0.99, 实体50%处]
        # 计算实体中轴
        limit_mid = (limit_bar['open'] + limit_bar['close']) / 2
        # 实战中不希望跌破涨停开盘价，跌破则视为走弱
        is_at_support = limit_bar['open'] * 0.99 <= last_price <= limit_mid
        
        if is_at_support:
            # 信号评级逻辑
            # 如果回调期间成交量持续下降 (Monotonic Decreasing)，评级为 SSS
            is_vol_down = retrace_df['volume'].is_monotonic_decreasing
            strength = "SSS" if is_vol_down else "A"
            
            return {
                "代码": code,
                "名称": name,
                "现价": last_price,
                "等级": strength,
                "回调天数": retrace_days,
                "量比": f"{v_ratio:.2f}",
                "支撑位": f"{limit_bar['open']:.2f}-{limit_mid:.2f}",
                "逻辑": f"首板后{retrace_days}天极致缩量，回踩实体下沿"
            }
            
    except Exception as e:
        return None

def run():
    stock_names = pd.read_csv('stock_names.csv', dtype={'code': str})
    names_map = dict(zip(stock_names['code'], stock_names['name']))
    
    files = glob.glob('stock_data/*.csv')
    results = []
    
    # 并行处理
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_stock, f, names_map) for f in files]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        res_df = pd.DataFrame(results)
        # 精选优加：按信号强度排序
        res_df = res_df.sort_values("信号强度", ascending=False)
        
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"FirstBoardRetracePro_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        res_df.to_csv(f"{dir_path}/{file_name}", index=False, encoding='utf-8-sig')
        print(f"筛选完成，找到 {len(res_df)} 只符合条件的股票。")
    else:
        print("今日未筛选出符合条件的个股。")

if __name__ == "__main__":
    run()
