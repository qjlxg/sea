import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：涨停低吸之高前三战法 (精选版)
# 核心逻辑：
# 1. 前期有涨停 + 缩量回调（主力在场，洗盘结束）
# 2. MA20 趋势向上 + 不破 20 日线（趋势支撑）
# 3. 高前三：今日收盘 > 过去3日最高（动能确认）
# 4. 严选买点：靠近 5 日均线 (±2%以内) 且温和放量
# ==========================================

DATA_DIR = './stock_data/'
NAME_FILE = 'stock_names.csv'
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path):
    try:
        code = os.path.basename(file_path).replace('.csv', '')
        if code.startswith(('30', '688', '4', '8', '2', 'ST', '*ST')):
            return None

        df = pd.read_csv(file_path)
        if len(df) < 60: return None # 增加数据长度以计算斜率
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        
        # 1. 基础硬约束：价格区间
        if not (PRICE_MIN <= last_close <= PRICE_MAX):
            return None

        # 计算指标
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        df['VOL_MA5'] = df['成交量'].rolling(window=5).mean()
        
        # 2. 涨停基因确认
        df['is_zt'] = df['涨跌幅'] >= 9.8
        if not df.iloc[-20:-2]['is_zt'].any(): # 排除今天涨停的，我们要买的是回调后的反转
            return None

        # 3. 趋势过滤 (MA20必须向上)
        ma20_slope = df.iloc[-1]['MA20'] > df.iloc[-5]['MA20']
        if not (last_close > df.iloc[-1]['MA20'] and ma20_slope):
            return None

        # 4. 高前三逻辑 (核心突破信号)
        prev_3_high = df.iloc[-4:-1]['最高'].max()
        is_high_3 = last_close > prev_3_high
        if not is_high_3:
            return None

        # 5. 精选买点优化 (只选靠近5日线的，且今日放量)
        dist_to_ma5 = (last_close - df.iloc[-1]['MA5']) / last_close
        vol_confirm = last_row['成交量'] > df.iloc[-2]['成交量'] # 今日量大于昨日量
        
        # 缩量回调判断 (前两日平均成交量 < 5日平均量) 代表洗盘缩量
        low_vol_back = df.iloc[-3:-1]['成交量'].mean() < df.iloc[-1]['VOL_MA5']

        if is_high_3 and vol_confirm and low_vol_back:
            # 最终打分逻辑
            if 0 <= dist_to_ma5 <= 0.02: # 严选：贴合5日线且收阳突破
                strength = "特强 (一击必中)"
                advice = "重点配置：当前位置极佳，贴合5日线起爆"
            elif -0.01 <= dist_to_ma5 < 0:
                strength = "高 (低吸机会)"
                advice = "分批建仓：回踩5日线未破"
            else:
                return None # 偏离太远直接剔除，避免结果过多

            return {
                "代码": code,
                "日期": last_row['日期'],
                "现价": last_close,
                "信号强度": strength,
                "操作建议": f"{advice} [止损点: {round(df.iloc[-1]['MA20'], 2)}]",
                "战法": "涨停低吸精选版"
            }
    except Exception:
        return None

def main():
    name_df = pd.read_csv(NAME_FILE, dtype={'code': str})
    name_dict = dict(zip(name_df['code'], name_df['name']))
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    print(f"开始并行精选 {len(files)} 只股票...")
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, files)
    
    final_list = [r for r in results if r is not None]
    
    if not final_list:
        print("今日严选条件下无符合信号。")
        return

    result_df = pd.DataFrame(final_list)
    result_df['名称'] = result_df['代码'].map(name_dict)
    
    # 按强度排序
    result_df = result_df[['日期', '代码', '名称', '现价', '信号强度', '操作建议', '战法']]
    
    now = datetime.now()
    dir_path = now.strftime('%Y-%m')
    if not os.path.exists(dir_path): os.makedirs(dir_path)
    
    file_path = os.path.join(dir_path, f"ZT_Low_Absorb_High3_{now.strftime('%Y%m%d')}.csv")
    result_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成！已从 1922 只中精选出 {len(result_df)} 只。结果见: {file_path}")

if __name__ == "__main__":
    main()
