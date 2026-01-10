import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：量价精选一击必中 (Extreme Volume Shrinkage Strategy)
复盘逻辑：
1. 强势基因：5日内有涨停或放量大涨（成交量 > 3倍均量）。
2. 极致缩量：今日成交量较放量日萎缩 60% 以上，且低于 10日均量。
3. 动态支撑：股价回踩 MA5，振幅收窄，处于变盘前夜。
4. 优中选优：全场只取评分前 5 名。
"""

INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def analyze_stock_strict(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 40: return None
        
        # 1. 基础硬性过滤
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None # 排除创业板
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        if not (MIN_PRICE <= last_close <= MAX_PRICE): return None

        # 2. 技术指标计算
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10_Vol'] = df['成交量'].rolling(10).mean()
        
        # 3. 寻找“放量基因” (5日内是否有显著放量)
        recent_5 = df.tail(6).head(5)
        max_vol_row = recent_5.loc[recent_5['成交量'].idxmax()]
        max_vol = max_vol_row['成交量']
        
        # 核心门槛：
        # A. 5日内必须有一次放量 (成交量是 10日均量的 2.5倍以上)
        if max_vol < max_vol_row['MA10_Vol'] * 2.5: return None
        
        # B. 极致缩量：今日量比放量日量 < 0.4 且低于 10日均量
        curr_vol = last_row['成交量']
        if curr_vol > max_vol * 0.4 or curr_vol > last_row['MA10_Vol']: return None
        
        # C. 价格支撑：收盘在MA5附近 (偏差 < 1%) 且 今日涨跌幅在 -2% 到 2% 之间（横盘震荡）
        ma5_val = last_row['MA5']
        if abs(last_close - ma5_val) / ma5_val > 0.01: return None
        if not (-2.5 <= last_row['涨跌幅'] <= 2.5): return None

        # 4. 评分系统
        score = 70
        if last_row['换手率'] > 3 and last_row['换手率'] < 7: score += 10 # 活跃度适中
        if last_close > last_row['开盘']: score += 5 # 收阳线加分
        if last_row['成交量'] < df['成交量'].iloc[-2]: score += 5 # 持续缩量加分

        # 操作建议
        if score >= 85:
            advice = "【精选一号】极致缩量+均线支撑，主力洗盘进入尾声，反弹概率极高。"
        else:
            advice = "【观察名单】形态尚可，建议控制仓位，等待量能再次放大信号。"

        return {
            "代码": code,
            "现价": last_close,
            "涨跌幅": f"{last_row['涨跌幅']}%",
            "换手率": f"{last_row['换手率']}%",
            "缩量比例": round(curr_vol / max_vol, 2),
            "评分": score,
            "操作建议": advice
        }
    except:
        return None

def main():
    files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains("ST|退")]
    valid_codes = set(names_df['code'].tolist())

    results = []
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_stock_strict, files):
            if res and res['代码'] in valid_codes:
                res['名称'] = names_df[names_df['code'] == res['代码']]['name'].values[0]
                results.append(res)

    # --- 关键改动：只保留评分前 5 名 ---
    results = sorted(results, key=lambda x: x['评分'], reverse=True)[:5]

    if results:
        df_final = pd.DataFrame(results)
        df_final = df_final[['代码', '名称', '评分', '操作建议', '现价', '涨跌幅', '缩量比例', '换手率']]
        
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        file_path = os.path.join(dir_name, f"volume_breakout_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成。优选 {len(df_final)} 只最强标的。")
    else:
        print("今日未发现极品信号。")

if __name__ == "__main__":
    main()
