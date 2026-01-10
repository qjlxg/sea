import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：涨停低吸之高前三战法
# 逻辑要领：
# 1. 前期有涨停（确认主力进场）
# 2. 回调不破20日线（确认上升趋势未坏）
# 3. 今日收盘价 > 前3日最高点（确认回调结束，反转启动）
# 4. 靠近5日线买入（买点优化）
# ==========================================

# 配置参数
DATA_DIR = './stock_data/'
NAME_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path):
    try:
        # 获取代码
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 排除非沪深A股 (排除30, 688, 4, 8, 2等)
        if code.startswith(('30', '688', '4', '8', '2', 'ST', '*ST')):
            return None

        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 基础过滤：最新价格区间
        last_close = df.iloc[-1]['收盘']
        if not (PRICE_MIN <= last_close <= PRICE_MAX):
            return None

        # 计算指标
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        
        # 1. 涨停基因：过去20天内是否有涨停 (A股10%限制)
        df['is_zt'] = df['涨跌幅'] >= 9.8
        has_zt = df.iloc[-20:]['is_zt'].any()
        
        # 2. 趋势条件：当前价格在MA20之上，且MA20趋势向上
        above_ma20 = last_close > df.iloc[-1]['MA20']
        
        # 3. 高前三逻辑：今日收盘价 > 过去3天的最高价
        prev_3_high = df.iloc[-4:-1]['最高'].max()
        is_high_3 = last_close > prev_3_high
        
        # 4. 辅助条件：当日处于回调后的反弹（不是连涨中）
        was_dropping = df.iloc[-2]['收盘'] < df.iloc[-3]['收盘'] or df.iloc[-2]['收盘'] < df.iloc[-2]['MA5']

        if has_zt and above_ma20 and is_high_3 and was_dropping:
            # 计算信号强度与建议
            # 距离5日线越近，买入评级越高
            dist_to_ma5 = abs(last_close - df.iloc[-1]['MA5']) / last_close
            
            strength = "高" if dist_to_ma5 < 0.02 else "中"
            advice = "分批试错" if strength == "高" else "观察等待回踩5日线"
            
            return {
                "代码": code,
                "日期": df.iloc[-1]['日期'],
                "现价": last_close,
                "信号强度": strength,
                "操作建议": f"{advice} (20日线止损)",
                "战法": "涨停低吸高前三"
            }
    except Exception as e:
        return None
    return None

def main():
    # 1. 加载名称映射
    name_df = pd.read_csv(NAME_FILE, dtype={'code': str})
    name_dict = dict(zip(name_df['code'], name_df['name']))

    # 2. 获取所有CSV文件路径
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 3. 并行处理
    print(f"开始分析 {len(files)} 只股票...")
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, files)
    
    # 4. 汇总结果
    final_list = [r for r in results if r is not None]
    if not final_list:
        print("今日无符合战法信号。")
        return

    result_df = pd.DataFrame(final_list)
    result_df['名称'] = result_df['代码'].map(name_dict)
    
    # 调整列顺序
    cols = ['日期', '代码', '名称', '现价', '信号强度', '操作建议', '战法']
    result_df = result_df[cols]

    # 5. 保存结果到年月目录
    now = datetime.now()
    dir_path = now.strftime('%Y-%m')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    file_name = f"ZT_Low_Absorb_High3_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    result_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成，优选结果已保存至: {save_path}")

if __name__ == "__main__":
    main()
