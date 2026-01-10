import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：成交量量价擒龙战法 (实战回归版)
核心逻辑（原汁原味）：
1. 识别放量：近期有单日大成交量（主力建仓信号）。
2. 识别缩量：放量后价格维持（不崩），量能快速萎缩（洗盘确认）。
3. 支撑确认：股价回落至 MA5 附近。
4. 择机入场：根据评分判断入场信号强度。
"""

# --- 配置 ---
INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 1. 基础过滤
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        if not (MIN_PRICE <= last_close <= MAX_PRICE): return None

        # 2. 计算关键指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10_Vol'] = df['成交量'].rolling(10).mean()
        
        # 寻找最近10日内的最大成交量作为“基准放量日”
        # 排除掉最近2天（我们要找的是洗盘了一几天的，不是刚放完量的）
        lookback_window = df.iloc[-12:-2] 
        if lookback_window.empty: return None
        
        base_vol_row = lookback_window.loc[lookback_window['成交量'].idxmax()]
        base_vol = base_vol_row['成交量']
        
        # --- 判定四步逻辑 ---
        
        # A. 识别放量 (只要比均量大1.8倍即可入围，不再苛求2.5倍)
        is_breakout = base_vol > base_vol_row['MA10_Vol'] * 1.8
        if not is_breakout: return None
        
        # B. 识别缩量 (今日成交量较基准放量萎缩 50% 以上)
        shrink_ratio = last_row['成交量'] / base_vol
        if shrink_ratio > 0.55: return None
        
        # C. 价格不崩 (收盘价不低于放量日开盘价的 97%)
        if last_close < base_vol_row['开盘'] * 0.97: return None
        
        # D. 支撑确认 (回踩 MA5 附近，容忍度放宽到 3%)
        ma5_val = last_row['MA5']
        dist_to_ma5 = (last_close - ma5_val) / ma5_val
        if not (-0.02 <= dist_to_ma5 <= 0.03): return None

        # --- 全自动复盘评分系统 ---
        score = 60 # 基础分
        
        # 加分项：极致缩量 (符合你图中说的缩量意味着分歧减小)
        if shrink_ratio < 0.3: score += 15
        # 加分项：MA5支撑精准度
        if abs(dist_to_ma5) < 0.01: score += 10
        # 加分项：今日收阳 (反转信号萌芽)
        if last_close > last_row['开盘']: score += 10
        # 加分项：换手率处于 2%-5% 的温和区
        if 2 <= last_row['换手率'] <= 5: score += 5

        # 操作建议逻辑
        if score >= 85:
            advice = "【一击必中】逻辑高度契合：缩量极致且精准踩准MA5，反弹一触即发。"
        elif score >= 70:
            advice = "【观察上车】逻辑符合：缩量回调到位，可考虑在MA5支撑位分批建仓。"
        else:
            advice = "【小幅试错】逻辑尚可：成交量有所萎缩，但价格波动仍需观察。"

        return {
            "代码": code,
            "评分": score,
            "操作建议": advice,
            "现价": last_close,
            "涨跌幅": f"{last_row['涨跌幅']}%",
            "缩量比": round(shrink_ratio, 2),
            "距MA5距离": f"{round(dist_to_ma5*100, 2)}%"
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
        for res in executor.map(analyze_logic, files):
            if res and res['代码'] in valid_codes:
                res['名称'] = names_df[names_df['code'] == res['代码']]['name'].values[0]
                results.append(res)

    # 排序：按评分从高到低
    results = sorted(results, key=lambda x: x['评分'], reverse=True)

    if results:
        # 结果适中：取前 15 名，既不刷屏，也不会一个没有
        df_final = pd.DataFrame(results[:15])
        
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        file_path = os.path.join(dir_name, f"volume_breakout_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成，已选出 {len(df_final)} 只符合战法逻辑的个股。")
    else:
        print("当前市场环境下，未匹配到完全符合放量缩量逻辑的个股。")

if __name__ == "__main__":
    main()
