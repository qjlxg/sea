import pandas as pd
import numpy as np
import os
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 战法配置区 ---
STRATEGY_NAME = "突破回踩一击必中"
PRICE_MIN = 5.0
PRICE_MAX = 20.0
DATA_DIR = "stock_data"
NAMES_FILE = "stock_names.csv"

def analyze_stock(file_path):
    """
    单只股票战法逻辑分析
    """
    try:
        code = os.path.basename(file_path).split('.')[0]
        
        # 排除 30 (创业板) 和 ST (通常文件名或数据内含)
        if code.startswith('30') or "ST" in code:
            return None
        
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 按照日期排序确保逻辑正确
        df = df.sort_values('日期')
        last_row = df.iloc[-1]
        
        # 1. 基础条件过滤
        close_price = last_row['收盘']
        if not (PRICE_MIN <= close_price <= PRICE_MAX):
            return None

        # 2. 战法逻辑计算
        # 计算均线
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['VOL_MA5'] = df['成交量'].rolling(5).mean()
        
        curr_close = last_row['收盘']
        curr_ma10 = last_row['MA10']
        
        # A. 寻找近期（5-10天内）是否有强力突破（涨幅>7% 且放量）
        recent_window = df.iloc[-10:-2]
        breakout_day = recent_window[(recent_window['涨跌幅'] > 7) & (recent_window['成交量'] > recent_window['VOL_MA5'] * 1.5)]
        
        if breakout_day.empty:
            return None
            
        # B. 回踩逻辑：当前价格靠近MA10或MA20，且近期成交量萎缩
        is_retracting = last_row['成交量'] < last_row['VOL_MA5']
        near_support = abs(curr_close - curr_ma10) / curr_ma10 < 0.02 # 距离均线2%以内
        
        if not (is_retracting and near_support):
            return None

        # 3. 评分系统：优中选优
        score = 0
        advice = "观察"
        
        if last_row['涨跌幅'] > 0: score += 20 # 回踩当日收阳
        if last_row['换手率'] < 5: score += 30 # 缩量回踩，主力未出
        if curr_close > last_row['MA20']: score += 20 # 趋势未破
        
        if score >= 60:
            advice = "试错（轻仓进场）"
        if score >= 80:
            advice = "重点击球（建议买入）"

        return {
            "代码": code,
            "收盘价": close_price,
            "涨跌幅": last_row['涨跌幅'],
            "信号强度": f"{score}%",
            "操作建议": advice,
            "战法描述": "放量大阳后缩量回调至支撑位"
        }
    except Exception as e:
        return None

def main():
    # 读取名称映射
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str), names_df['name']))
    
    # 获取所有待处理文件
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # 并行处理
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    # 过滤空结果
    results = [r for r in results if r is not None]
    
    if results:
        final_df = pd.DataFrame(results)
        # 匹配股票名称
        final_df['股票名称'] = final_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        # 整理目录结构
        now = datetime.now()
        dir_path = now.strftime("%Y-%m")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            
        file_name = f"{dir_path}/breakback_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"分析完成，识别出 {len(final_df)} 只符合战法标的。")
    else:
        print("今日无符合突破回踩条件的股票。")

if __name__ == "__main__":
    main()
