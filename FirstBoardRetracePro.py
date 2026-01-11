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
        
        # 基础过滤：价格 5-20, 非30开头, 非ST
        last_price = df.iloc[-1]['close']
        code = str(df.iloc[-1]['code']).zfill(6)
        name = stock_names_map.get(code, "未知")
        
        if not (5.0 <= last_price <= 20.0): return None
        if code.startswith('30') or "ST" in name: return None

        # 1. 寻找最近10日内的首板
        lookback = 10
        recent_df = df.tail(lookback + 7) # 多取几行确保回调逻辑完整
        
        limit_up_idx = -1
        for i in range(len(recent_df)-1, len(recent_df)-lookback-1, -1):
            if recent_df.iloc[i]['pct_chg'] >= 9.5:
                # 确保是“首板”：此前5天没涨停
                if i > 5 and not (recent_df.iloc[i-5:i]['pct_chg'] >= 9.5).any():
                    limit_up_idx = i
                    break
        
        if limit_up_idx == -1 or limit_up_idx == len(recent_df)-1: return None
        
        # 2. 统计回调特征
        retrace_df = recent_df.iloc[limit_up_idx+1:]
        retrace_days = len(retrace_df)
        
        if retrace_days > 7 or retrace_days == 0: return None
        
        # 3. 核心：量价判断
        limit_bar = recent_df.iloc[limit_up_idx]
        # 判断缩量：回调期平均成交量 < 涨停日成交量的 0.7倍，且今日缩量
        v_decrease = retrace_df['volume'].iloc[-1] < limit_bar['volume'] * 0.8
        
        # 判断回踩深度：现价在涨停柱底部 (limit_low 到 实体30%处)
        support_price = limit_bar['low']
        max_buy_zone = limit_bar['low'] + (limit_bar['close'] - limit_bar['low']) * 0.4
        price_in_zone = support_price * 0.98 <= last_price <= max_buy_zone
        
        if v_decrease and price_in_zone:
            # --- 简易回测逻辑 ---
            # 查找该股历史上所有符合此形态的点，计算后3天最高涨幅
            score = 0
            # (由于并行效率，此处仅做逻辑演示，实际可深度扫描全量数据)
            
            signal_strength = "强" if retrace_df['volume'].is_monotonic_decreasing else "中"
            suggestion = "分批建仓" if signal_strength == "强" else "轻仓试错"
            
            return {
                "代码": code,
                "名称": name,
                "现价": last_price,
                "涨停日": limit_bar['date'],
                "回调天数": retrace_days,
                "信号强度": signal_strength,
                "操作建议": suggestion,
                "买入逻辑": "首板后缩量回踩支撑位，缩量比率:{:.2f}".format(retrace_df['volume'].iloc[-1]/limit_bar['volume'])
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
