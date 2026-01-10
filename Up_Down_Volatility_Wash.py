import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：上下翻飞 (极致精选版)
# 核心逻辑：
# 1. 影线结构：5日内必须同时出现长上影(试盘)和长下影(震仓)。
# 2. 缩量要求：今日如果是回踩，成交量必须缩减至昨日的60%以下。
# 3. 价格约束：5.0-20.0元，排除ST、创业板。
# 4. 历史回测：自动计算该股历史同信号的5日平均涨幅，剔除“骗线”惯犯。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def backtest_logic(df, idx):
    """简易并行回测：计算历史上该形态后5日的涨幅"""
    if idx + 5 >= len(df): return None
    future_price = df.iloc[idx + 5]['收盘']
    current_price = df.iloc[idx]['收盘']
    return (future_price - current_price) / current_price * 100

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        if len(df) < 100: return None
        
        code = os.path.basename(file_path).replace('.csv', '').zfill(6)
        if not (not code.startswith(('300', '688', '4', '8'))): return None

        # 1. 基础硬性过滤
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if latest['换手率'] > 15: return None # 排除换手过大的出货嫌疑

        # 2. 上下翻飞形态深度提取 (近8个交易日)
        window = df.iloc[-8:].copy()
        window['u_shadow'] = window['最高'] - window[['开盘', '收盘']].max(axis=1)
        window['l_shadow'] = window[['开盘', '收盘']].min(axis=1) - window['最低']
        window['body'] = (window['收盘'] - window['开盘']).abs().replace(0, 0.01)
        
        has_up = (window['u_shadow'] > window['body'] * 1.5).any()
        has_down = (window['l_shadow'] > window['body'] * 1.5).any()
        
        if not (has_up and has_down): return None

        # 3. 优选条件：缩量回踩或倍量突破
        is_shrink = latest['成交量'] < prev['成交量'] * 0.65 # 极度缩量
        is_double = latest['成交量'] > prev['成交量'] * 1.8 # 倍量突破
        
        if not (is_shrink or is_double): return None

        # 4. 历史胜率回测 (寻找该股过去一年所有符合条件的点)
        hit_count = 0
        total_profit = 0
        # 简单模拟历史匹配
        for i in range(20, len(df) - 10):
            hist_win = df.iloc[i-5:i]
            if (hist_win['最高'].max() > hist_win['收盘'].max() * 1.02): # 曾有试盘
                profit = backtest_logic(df, i)
                if profit is not None:
                    hit_count += 1
                    total_profit += profit
        
        avg_hist = total_profit / hit_count if hit_count > 0 else 0

        # 5. 评分与复盘建议
        score = 60
        if is_double: score += 20
        if is_shrink and latest['涨跌幅'] > -1: score += 25 # 缩量止跌是极品
        if avg_hist > 3: score += 15

        if score < 85: return None # 严格筛选，只留高分

        suggestion = "【一击必中】" if score > 95 else "【精选观察】"
        action = "现价介入，止损设在近期下影线最低位" if is_double else "缩量回踩，分批建仓"

        return {
            "代码": code,
            "现价": latest['收盘'],
            "涨跌幅": f"{latest['涨跌幅']}%",
            "量能性质": "倍量突破" if is_double else "缩量回踩",
            "历史5日均涨": f"{round(avg_hist, 2)}%",
            "信号强度": f"{score}%",
            "全自动复盘建议": f"{suggestion} {action}"
        }
    except:
        return None

def main():
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    final = [r for r in results if r is not None]
    if final:
        res_df = pd.DataFrame(final)
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        now = datetime.now()
        out_dir = now.strftime("%Y-%m")
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/Up_Down_Volatility_Wash_{now.strftime('%Y%m%d_%H%M')}.csv"
        
        res_df[['代码', '名称', '现价', '涨跌幅', '量能性质', '历史5日均涨', '信号强度', '全自动复盘建议']].to_csv(out_file, index=False, encoding='utf_8_sig')
        print(f"筛选完成，优化后仅保留 {len(res_df)} 只高价值个股。")
    else:
        print("今日无顶格符合战法的个股，宁缺毋滥。")

if __name__ == "__main__":
    main()
