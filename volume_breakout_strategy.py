import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：成交量量价擒龙战法 (实战回归+RSI风控+自适应参数版)
核心逻辑（100% 还原）：
1. 识别放量：寻找基准大阳柱（主力建仓）。
2. 识别缩量：放量后股价横盘不崩，量能萎缩（洗盘确认）。
3. 支撑确认：股价回落至 MA5 附近且不跌破（买点）。
4. RSI风控：监控 RSI(14) 情绪，防止买在超买止盈点。
"""

# --- 基础配置 ---
INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def calculate_rsi(series, period=14):
    """计算 RSI 指标 (14日)"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 40: return None
        
        # 1. 基础过滤：排除创业板(30)和价格区间外
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        if not (MIN_PRICE <= last_close <= MAX_PRICE): return None

        # 2. 指标计算
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10_Vol'] = df['成交量'].rolling(10).mean()
        df['RSI'] = calculate_rsi(df['收盘'], 14)
        
        # --- 四步核心逻辑 (自适应参数优化版) ---
        # 寻找最近15日内的最大成交量作为“基准放量日” (扩大搜索深度)
        lookback_window = df.iloc[-17:-2] 
        if lookback_window.empty: return None
        
        base_vol_row = lookback_window.loc[lookback_window['成交量'].idxmax()]
        base_vol = base_vol_row['成交量']
        
        # A. 识别放量 (主力入场痕迹)
        is_breakout = base_vol > base_vol_row['MA10_Vol'] * 1.5 # 放宽至1.5倍
        if not is_breakout: return None
        
        # B. 识别缩量 (今日成交量较基准放量萎缩 30%-70% 之间)
        shrink_ratio = last_row['成交量'] / base_vol
        if shrink_ratio > 0.70: return None # 放宽至0.7
        
        # C. 价格不崩 (不低于放量日开盘价的 95%)
        if last_close < base_vol_row['开盘'] * 0.95: return None
        
        # D. 支撑确认 (偏离 MA5 上下 4% 均视为支撑有效)
        ma5_val = last_row['MA5']
        dist_to_ma5 = (last_close - ma5_val) / ma5_val
        if not (-0.04 <= dist_to_ma5 <= 0.04): return None

        # --- 全自动复盘评分与 RSI 风控系统 ---
        score = 60 # 基础起步分
        
        # 细节加分项
        if shrink_ratio < 0.35: score += 15 # 极致缩量
        if abs(dist_to_ma5) < 0.01: score += 10 # 极准回踩
        if last_close > last_row['开盘']: score += 10 # 收阳线信号
        if 2 <= last_row['换手率'] <= 6: score += 5 # 适中换手

        # RSI 情绪位分级与风控 (核心补全功能)
        current_rsi = round(last_row['RSI'], 2)
        if current_rsi >= 80:
            risk_level = "极高（超买）"
            risk_advice = "止盈警示：RSI超80，属于战法中的撤离区，拒绝开仓。"
            score -= 40 # 极度危险减分
        elif current_rsi >= 70:
            risk_level = "偏高"
            risk_advice = "风控提醒：高位钝化，若不放量反包则需离场。"
        elif current_rsi <= 35:
            risk_level = "超卖"
            risk_advice = "观察：底背离机会，等待放量确认。"
        else:
            risk_level = "安全"
            risk_advice = "买入参考：情绪位健康，符合缩量回踩逻辑。"

        # 操作建议
        if score >= 85:
            final_advice = "【一击必中】量价逻辑完美，RSI水位极佳，重点关注。"
        elif score >= 70:
            final_advice = "【观察上车】符合缩量特征，回踩到位，可轻仓试探。"
        else:
            final_advice = "【技术备选】逻辑尚可，但动能或情绪稍欠。"

        return {
            "代码": code,
            "评分": score,
            "操作建议": final_advice,
            "现价": last_close,
            "涨跌幅": f"{last_row['涨跌幅']}%",
            "RSI14": current_rsi,
            "情绪水位": risk_level,
            "买卖风控建议": risk_advice,
            "缩量比": round(shrink_ratio, 2),
            "距MA5距离": f"{round(dist_to_ma5*100, 2)}%"
        }
    except:
        return None

def main():
    files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    # 加载名称
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains("ST|退")]
    valid_codes = set(names_df['code'].tolist())

    results = []
    # 并行处理
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_logic, files):
            if res and res['代码'] in valid_codes:
                res['名称'] = names_df[names_df['code'] == res['代码']]['name'].values[0]
                results.append(res)

    # 按评分降序排列
    results = sorted(results, key=lambda x: x['评分'], reverse=True)

    if results:
        # 只保留最精华的前 15 名
        df_final = pd.DataFrame(results[:15])
        
        # 动态创建文件夹
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        file_path = os.path.join(dir_name, f"Vol_Master_Final_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成！已选出 {len(df_final)} 只符合【量价擒龙】战法的优质标的。")
        print(f"文件位置：{file_path}")
    else:
        print("当前市场环境下，未匹配到符合战法基因的个股，建议空仓观察。")

if __name__ == "__main__":
    main()
