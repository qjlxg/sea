import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：成交量量价擒龙战法 (实战回归+RSI风控版)
核心逻辑：
1. 识别放量：近期有单日大成交量（主力入场）。
2. 识别缩量：放量后价格横盘不崩，量能萎缩 50% 以上（洗盘确认）。
3. 支撑确认：股价回落至 MA5 附近（买点）。
4. RSI风控：利用 RSI 指标监控情绪高位，防止追涨在止盈点。
"""

# --- 配置 ---
INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def calculate_rsi(series, period=14):
    """计算 RSI 指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 40: return None
        
        # 1. 基础过滤
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        if not (MIN_PRICE <= last_close <= MAX_PRICE): return None

        # 2. 计算关键指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10_Vol'] = df['成交量'].rolling(10).mean()
        df['RSI'] = calculate_rsi(df['收盘'], 14)
        
        # 寻找最近10日内的最大成交量作为“基准放量日”
        # 排除掉最近2天（找洗盘中的，不找刚放量的）
        lookback_window = df.iloc[-22:-2] 
        if lookback_window.empty: return None
        
        base_vol_row = lookback_window.loc[lookback_window['成交量'].idxmax()]
        base_vol = base_vol_row['成交量']
        
        # --- 判定战法核心逻辑 ---
        
        # A. 识别放量 (主力入场痕迹)
        is_breakout = base_vol > base_vol_row['MA10_Vol'] * 1.8
        if not is_breakout: return None
        
        # B. 识别缩量 (今日成交量较基准放量萎缩 50% 以上)
        shrink_ratio = last_row['成交量'] / base_vol
        if shrink_ratio > 0.55: return None
        
        # C. 价格不崩 (收盘价不低于放量日开盘价的 97%)
        if last_close < base_vol_row['开盘'] * 0.97: return None
        
        # D. 支撑确认 (回踩 MA5 附近)
        ma5_val = last_row['MA5']
        dist_to_ma5 = (last_close - ma5_val) / ma5_val
        if not (-0.04 <= dist_to_ma5 <= 0.03): return None

        # --- 全自动复盘评分与 RSI 风控系统 ---
        score = 60 
        
        # 基础评分逻辑
        if shrink_ratio < 0.3: score += 15
        if abs(dist_to_ma5) < 0.01: score += 10
        if last_close > last_row['开盘']: score += 10
        if 2 <= last_row['换手率'] <= 5: score += 5

        # RSI 情绪与风控逻辑 (对应战法第三阶段)
        current_rsi = round(last_row['RSI'], 2)
        if current_rsi >= 80:
            risk_level = "极高（超买区）"
            risk_advice = "止盈参考：股价处于超买，谨防见顶回落，不宜开仓。"
            score -= 30 # RSI 过高大幅减分
        elif current_rsi >= 70:
            risk_level = "偏高"
            risk_advice = "风控提示：情绪高涨，若缩量回踩无力则随时离场。"
        elif current_rsi <= 30:
            risk_level = "超卖"
            risk_advice = "风控提示：市场极度低迷，注意止跌信号。"
        else:
            risk_level = "正常"
            risk_advice = "买入参考：情绪稳定，符合缩量回踩逻辑。"

        # 操作建议生成
        if score >= 85:
            final_advice = "【一击必中】逻辑高度契合，风控安全，反弹概率极高。"
        elif score >= 70:
            final_advice = "【观察上车】形态符合，情绪正常，建议MA5支撑位分批试探。"
        else:
            final_advice = "【暂时放弃】逻辑虽在但风险较高或动能不足。"

        return {
            "代码": code,
            "评分": score,
            "操作建议": final_advice,
            "现价": last_close,
            "涨跌幅": f"{last_row['涨跌幅']}%",
            "RSI14": current_rsi,
            "情绪位": risk_level,
            "买卖风控建议": risk_advice,
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

    results = sorted(results, key=lambda x: x['评分'], reverse=True)

    if results:
        df_final = pd.DataFrame(results[:15])
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        file_path = os.path.join(dir_name, f"volume_breakout_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成，已选出 {len(df_final)} 只符合战法逻辑并结合RSI风控的个股。")
    else:
        print("当前市场环境下，未匹配到完全符合放量缩量逻辑的个股。")

if __name__ == "__main__":
    main()
