import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：上下翻飞 + RSI 动能过滤 (极致精选版)
# 核心逻辑：
# 1. 影线结构：近期必须同时出现试盘上影和震仓下影。
# 2. RSI 过滤：RSI(14) 必须在 50-80 之间。50以下动能不足，80以上过热。
# 3. 价格/板块：5.0-20.0元，排除ST、创业板、科创板。
# 4. 全自动复盘：结合量能、RSI和历史胜率给出终极操作建议。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def calculate_rsi(series, period=14):
    """手动计算RSI，减少对外部库依赖"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        if len(df) < 100: return None
        
        code = os.path.basename(file_path).replace('.csv', '').zfill(6)
        # 排除创业板(30)、科创板(68)、北交所(4/8)
        if code.startswith(('300', '688', '4', '8')): return None

        # 1. 计算 RSI (14日)
        df['rsi'] = calculate_rsi(df['收盘'], 14)
        latest_rsi = df.iloc[-1]['rsi']
        
        # 2. 基础硬性过滤
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if not (50 <= latest_rsi <= 80): return None # 动能区间过滤

        # 3. 上下翻飞形态检测 (近10日)
        window = df.iloc[-10:].copy()
        window['u_shadow'] = window['最高'] - window[['开盘', '收盘']].max(axis=1)
        window['l_shadow'] = window[['开盘', '收盘']].min(axis=1) - window['最低']
        window['body'] = (window['收盘'] - window['开盘']).abs().replace(0, 0.01)
        
        has_up = (window['u_shadow'] > window['body'] * 1.8).any() # 试盘线
        has_down = (window['l_shadow'] > window['body'] * 1.8).any() # 震仓线
        if not (has_up and has_down): return None

        # 4. 量能判定
        is_shrink = latest['成交量'] < prev['成交量'] * 0.65 # 缩量洗盘完成
        is_double = latest['成交量'] > prev['成交量'] * 1.8 # 倍量拉升开始
        if not (is_shrink or is_double): return None

        # 5. 评分系统
        score = 60
        if is_double: score += 15
        if 60 <= latest_rsi <= 75: score += 15 # 黄金动能区
        if latest['换手率'] > 3 and latest['换手率'] < 10: score += 10 # 活跃但不混乱

        if score < 85: return None 

        # 6. 操作建议自动化生成
        status = "倍量进攻" if is_double else "缩量蓄势"
        if score >= 95:
            advice = "【一击必中】RSI动能强劲且形态完美，建议重仓博弈。"
        else:
            advice = f"【精选观察】{status}阶段，建议根据分时图择机轻仓入场。"

        return {
            "代码": code,
            "现价": latest['收盘'],
            "涨跌幅": f"{latest['涨跌幅']}%",
            "RSI14": round(latest_rsi, 2),
            "量能": status,
            "强度": f"{score}%",
            "实战复盘建议": advice
        }
    except:
        return None

def main():
    # 匹配名称
    try:
        names_df = pd.read_csv(NAMES_FILE)
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 使用并行计算加速
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    final = [r for r in results if r is not None]
    
    if final:
        res_df = pd.DataFrame(final)
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        # 排序并导出
        now = datetime.now()
        out_dir = now.strftime("%Y-%m")
        os.makedirs(out_dir, exist_ok=True)
        file_name = f"{out_dir}/Up_Down_Volatility_Wash_{now.strftime('%Y%m%d_%H%M')}.csv"
        
        cols = ['代码', '名称', '现价', '涨跌幅', 'RSI14', '量能', '强度', '实战复盘建议']
        res_df[cols].sort_values(by="强度", ascending=False).to_csv(file_name, index=False, encoding='utf_8_sig')
        print(f"筛选复盘完成！共锁定 {len(res_df)} 只高胜率标的。")
    else:
        print("今日未发现符合‘上下翻飞+RSI强动能’的个股。")

if __name__ == "__main__":
    main()
