import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

"""
战法名称：破晓龙回头 (Breakout Dragon System)
核心逻辑：
1. 筛选条件：5元 < 收盘价 < 20元；排除ST、创业板(30开头)。
2. 形态逻辑：寻找股价在低位构筑的“水平压力位”，且最新交易日通过大阳线带量突破。
3. 复盘要领：只做突破瞬间。若换手率过高则谨防诱多，若成交量不足则视为假突破。
"""

def analyze_stock(file_path, name_dict):
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 30: return None
        
        # 基础数据清洗
        df = df.sort_values('日期')
        code = os.path.basename(file_path).replace('.csv', '')
        
        # 1. 排除规则
        if code.startswith('30'): return None  # 排除创业板
        stock_name = name_dict.get(code, "未知")
        if "ST" in stock_name: return None     # 排除ST
        
        last_row = df.iloc[-1]
        close_price = last_row['收盘']
        
        # 2. 价格区间限制 (5.0 - 20.0)
        if not (5.0 <= close_price <= 20.0): return None
        
        # --- 战法核心计算 ---
        # 计算过去20天的压力位（最高价的均值或局部高点）
        history = df.iloc[-21:-1]
        pressure_line = history['最高'].max()
        avg_volume = history['成交量'].mean()
        
        # 突破逻辑：收盘价高于前20日最高价，且涨幅大于3%
        is_breakout = close_price > pressure_line and last_row['涨跌幅'] > 3.0
        # 量能逻辑：今日成交量需是过去20日平均量的1.5倍以上
        volume_ratio = last_row['成交量'] / avg_volume if avg_volume > 0 else 0
        
        if is_breakout and volume_ratio > 1.5:
            # 评估买入强度
            strength = 0
            if last_row['换手率'] > 5: strength += 40
            if last_row['涨跌幅'] > 7: strength += 30
            if volume_ratio > 2.5: strength += 30
            
            # 操作建议逻辑
            suggestion = ""
            if strength >= 80:
                suggestion = "【一击必中】核心标的，放量突破确立，建议重仓关注。"
            elif strength >= 60:
                suggestion = "【谨慎试错】趋势转强，量能尚可，建议小仓位博弈。"
            else:
                suggestion = "【观察等待】虽有突破但力度一般，建议加入自选观察回踩。"
                
            return {
                "日期": last_row['日期'],
                "代码": code,
                "名称": stock_name,
                "收盘价": close_price,
                "涨跌幅": last_row['涨跌幅'],
                "成交倍率": round(volume_ratio, 2),
                "换手率": last_row['换手率'],
                "买入信号强度": f"{strength}%",
                "操作建议": suggestion
            }
    except Exception as e:
        return None
    return None

def main():
    # 加载股票名称
    try:
        names_df = pd.read_csv('stock_names.csv')
        # 确保代码格式为字符串并补齐6位
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        name_dict = dict(zip(names_df['code'], names_df['name']))
    except:
        name_dict = {}

    # 并行扫描目录
    stock_files = glob.glob('stock_data/*.csv')
    print(f"开始扫描 {len(stock_files)} 个数据文件...")
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, name_dict) for f in stock_files])
    
    # 过滤空结果
    final_list = [r for r in results if r is not None]
    
    # 结果处理
    if final_list:
        output_df = pd.DataFrame(final_list)
        # 按强度排序
        output_df = output_df.sort_values("买入信号强度", ascending=False)
        
        # 创建年月目录
        now = datetime.now()
        dir_path = now.strftime('%Y%m')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            
        # 保存文件
        file_name = f"breakout_dragon_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = os.path.join(dir_path, file_name)
        output_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成，筛选出 {len(final_list)} 只潜力股。结果已保存至 {full_path}")
    else:
        print("今日未匹配到符合“破晓龙回头”战法的优选个股。")

if __name__ == "__main__":
    main()
