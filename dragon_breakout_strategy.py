import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import concurrent.futures

# ==========================================
# 战法名称：龙回头蓄势突破战法
# 操作要领：
# 1. 均线为王：MA60/MA120 支撑不破。
# 2. 缩量洗盘：起爆前必有极度缩量（地量）。
# 3. 换手接力：今日换手率 3%-15% 为佳。
# 4. 价格区间：5-20元，剔除ST与创业板。
# ==========================================

STRATEGY_NAME = "dragon_breakout_strategy"
DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"
OUTPUT_DIR_BASE = datetime.now().strftime("%Y-%m")

def analyze_stock(file_path, name_map):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 130:  # 确保有足够计算均线的数据
            return None
        
        # 基础属性过滤
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        # 排除 30 开头 (创业板) 和 排除 ST (通过名称判断，需结合name_map)
        if code.startswith('30'):
            return None
        
        name = name_map.get(code, "未知")
        if "ST" in name or "*" in name:
            return None

        # 最新指标
        last_close = df['收盘'].iloc[-1]
        last_pct_change = df['涨跌幅'].iloc[-1]
        last_turnover = df['换手率'].iloc[-1]
        last_vol = df['成交量'].iloc[-1]
        
        # 价格过滤 5.0 - 20.0
        if not (5.0 <= last_close <= 20.0):
            return None

        # 计算指标
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        df['MA60'] = df['收盘'].rolling(window=60).mean()
        df['MA120'] = df['收盘'].rolling(window=120).mean()
        df['VOL_MA5'] = df['成交量'].rolling(window=5).mean()
        df['VOL_MA20'] = df['成交量'].rolling(window=20).mean()

        # 战法逻辑判定
        # 1. 均线支撑：股价在60日和120日线上方
        is_bull_trend = last_close > df['MA60'].iloc[-1] and last_close > df['MA120'].iloc[-1]
        
        # 2. 缩量洗盘判定：前5日内有低地量
        recent_vols = df['成交量'].iloc[-6:-1]
        has_low_vol = any(recent_vols < df['VOL_MA20'].iloc[-1] * 0.7)
        
        # 3. 今日放量确认：成交量是5日均量的1.5倍以上
        vol_breakout = last_vol > df['VOL_MA5'].iloc[-1] * 1.5
        
        # 4. 涨幅过滤：主板强势但不宜过早封死或大幅跳空，设定为 3% - 10.5%
        is_price_ok = 3.0 <= last_pct_change <= 10.5

        if is_bull_trend and has_low_vol and is_price_ok and vol_breakout:
            # 信号强度评分
            score = 0
            if last_close > df['MA20'].iloc[-1]: score += 1
            if last_vol > df['VOL_MA20'].iloc[-1] * 2: score += 2
            if last_turnover < 15: score += 2
            
            strength = f"{score} 级"
            suggestion = "暂时观察"
            if score >= 4:
                suggestion = "一击必中：强力买入"
            elif score >= 2:
                suggestion = "试错介入：分批建仓"
            
            # 历史回测简单逻辑：计算未来3天的最高涨幅（此处为展示，实战需后验数据）
            # 仅作为模拟，输出当前筛选结果
            return {
                "日期": df['日期'].iloc[-1],
                "代码": code,
                "名称": name,
                "收盘价": last_close,
                "涨跌幅": last_pct_change,
                "换手率": last_turnover,
                "信号强度": strength,
                "操作建议": suggestion
            }
    except Exception as e:
        return None
    return None

def main():
    # 加载名称映射
    name_df = pd.read_csv(NAMES_FILE)
    name_df['code'] = name_df['code'].astype(str).str.zfill(6)
    name_map = dict(zip(name_df['code'], name_df['name']))

    # 并行扫描 CSV
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    results = []
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_stock = {executor.submit(analyze_stock, f, name_map): f for f in files}
        for future in concurrent.futures.as_completed(future_to_stock):
            res = future.result()
            if res:
                results.append(res)

    # 结果处理与保存
    if results:
        res_df = pd.DataFrame(results)
        # 优中选优：按强度排序
        res_df = res_df.sort_values(by="信号强度", ascending=False).head(5) 
        
        os.makedirs(OUTPUT_DIR_BASE, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{STRATEGY_NAME}_{timestamp}.csv"
        save_path = os.path.join(OUTPUT_DIR_BASE, file_name)
        
        res_df.to_csv(save_path, index=False, encoding='utf_8_sig')
        print(f"复盘完成，筛选出 {len(res_df)} 只精选标的。结果保存至: {save_path}")
    else:
        print("今日无符合'龙回头'战法标的，建议空仓休息。")

if __name__ == "__main__":
    main()
