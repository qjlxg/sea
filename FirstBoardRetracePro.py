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
        df.columns = ['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'pct_chg', 'change', 'turnover']
        df = df.sort_values('date')
        
        if len(df) < 20: 
            return None
        
        last_bar = df.iloc[-1]
        last_price = last_bar['close']
        code = str(last_bar['code']).zfill(6)
        name = stock_names_map.get(code, "未知")
        
        # 1. 基础过滤：主板, 非ST, 价格区间
        if not (5.0 <= last_price <= 35.0): return None
        if code.startswith(('30', '688')) or "ST" in name: return None

        # 2. 寻找最近10日内的首板
        lookback = 10
        recent_df = df.tail(lookback + 7).reset_index(drop=True)
        
        limit_up_idx = -1
        for i in range(len(recent_df)-1, len(recent_df)-lookback-1, -1):
            if recent_df.iloc[i]['pct_chg'] >= 9.8:
                # 确保首板：此前5天无涨停
                if i > 5 and not (recent_df.iloc[i-5:i]['pct_chg'] >= 9.8).any():
                    limit_up_idx = i
                    break
        
        if limit_up_idx == -1 or limit_up_idx == len(recent_df)-1: 
            return None
        
        # 3. 统计回调特征
        retrace_df = recent_df.iloc[limit_up_idx+1:]
        retrace_days = len(retrace_df)
        
        # 实战收紧：黄金回调期 2-5 天
        if not (2 <= retrace_days <= 5): 
            return None
        
        # 4. 量价判定
        limit_bar = recent_df.iloc[limit_up_idx]
        v_ratio = last_bar['volume'] / limit_bar['volume']
        
        # 极致缩量要求 (0.15 - 0.6 之间)
        v_decrease = 0.15 <= v_ratio <= 0.60
        
        # 价格回踩区间：不破涨停开盘价，回踩至实体中轴
        limit_mid = (limit_bar['open'] + limit_bar['close']) / 2
        price_in_zone = limit_bar['open'] * 0.995 <= last_price <= limit_mid
        
        # 回调质量：无大阴线
        no_big_down = not (retrace_df['pct_chg'] < -6.0).any()
        
        if v_decrease and price_in_zone and no_big_down:
            is_monotonic = retrace_df['volume'].is_monotonic_decreasing
            signal_strength = "强" if is_monotonic else "中"
            
            return {
                "代码": code,
                "名称": name,
                "现价": last_price,
                "涨停日": limit_bar['date'],
                "回调天数": retrace_days,
                "信号强度": signal_strength,
                "操作建议": "每只6000元" if signal_strength == "强" else "轻仓观察",
                "买入逻辑": f"回踩实体中轴，量能腰斩({v_ratio:.2f})"
            }
            
    except Exception as e:
        return None
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
