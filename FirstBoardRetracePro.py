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
        
        if len(df) < 20: return None
        
        last_bar = df.iloc[-1]
        last_price = last_bar['close']
        code = str(last_bar['code']).zfill(6)
        name = stock_names_map.get(code, "未知")
        
        # 基础过滤保持不变
        if not (5.0 <= last_price <= 35.0): return None
        if code.startswith('30') or "ST" in name: return None

        # 1. 寻找最近10日内的首板 (逻辑微调：确保涨停质量)
        lookback = 10
        recent_df = df.tail(lookback + 7).reset_index(drop=True)
        
        limit_up_idx = -1
        for i in range(len(recent_df)-1, len(recent_df)-lookback-1, -1):
            if recent_df.iloc[i]['pct_chg'] >= 9.8: # 强化为9.8%以上
                # 确保是首板：此前5天无涨停
                if i > 5 and not (recent_df.iloc[i-5:i]['pct_chg'] >= 9.8).any():
                    limit_up_idx = i
                    break
        
        if limit_up_idx == -1 or limit_up_idx == len(recent_df)-1: return None
        
        # 2. 统计回调特征 - 收紧时间窗口
        retrace_df = recent_df.iloc[limit_up_idx+1:]
        retrace_days = len(retrace_df)
        
        # 实战调优：将上限从 7 天改为 5 天 (3-5天是短线回踩的黄金期)
        if not (2 <= retrace_days <= 5): return None
        
        # 3. 核心：量价微调
        # A. 增加异常量过滤 (今日量不能太小，防止停牌或数据缺失)
        v_ratio = last_bar['volume'] / limit_bar['volume']
        if v_ratio < 0.15 or v_ratio > 0.65: return None # 只留缩量在 1.5成 到 6.5成 之间的
        
        # B. 价格支撑点更加精确 (涨停实体的 10% - 50% 处)
        limit_bottom = limit_bar['open']
        limit_mid = (limit_bar['open'] + limit_bar['close']) / 2
        # 价格必须落在涨停板开盘价上方一点点，到中轴之间
        price_in_zone = limit_bottom * 0.995 <= last_price <= limit_mid
        
        # C. 质量过滤：回调期间严禁出现大阴线 (跌幅 > 6%)
        no_crash = not (retrace_df['pct_chg'] < -6.0).any()
        
        if v_decrease and price_in_zone and no_crash:
            # 信号分级：如果成交量是持续递减的，定为“强”
            is_monotonic = retrace_df['volume'].is_monotonic_decreasing
            signal_strength = "强" if is_monotonic else "中"
            
            return {
                "代码": code,
                "名称": name,
                "现价": last_price,
                "涨停日": limit_bar['date'],
                "回调天数": retrace_days,
                "信号强度": signal_strength,
                "操作建议": "低吸关注" if signal_strength == "强" else "轻仓观察",
                "买入逻辑": f"回踩实体中轨，量能萎缩比:{last_bar['volume']/limit_bar['volume']:.2f}"
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
