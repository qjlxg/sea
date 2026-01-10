import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 战法名称：龙头二板首阴 (Dragon Return Strategy)
# 逻辑核心：连板龙头股首个放量阴线洗盘，不破缺口，等待缩量反包

def analyze_stock(file_path, names_df):
    try:
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除 ST, 创业板(30), 科创板(68)
        if code.startswith(('30', '68')) or "ST" in names_df.get(code, ""):
            return None

        df = pd.read_csv(file_path)
        if len(df) < 10: return None
        
        # 转换日期并排序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').tail(20) # 取最近20天数据

        last_row = df.iloc[-1]
        curr_price = last_row['收盘']
        
        # 基础过滤：价格区间
        if not (5.0 <= curr_price <= 20.0):
            return None

        # 战法逻辑计算
        # 计算涨幅
        df['pct_chg'] = df['涨跌幅']
        
        # 1. 寻找二连板 (前天和前前天涨停)
        # 假设 A 股涨停基准为 9.5%
        is_limit_up = df['pct_chg'] >= 9.5
        
        # 检查倒数第2, 3天是否连板
        if not (is_limit_up.iloc[-2] and is_limit_up.iloc[-3]):
            return None

        # 2. 检查第二个涨停是否为一字板
        is_one_word = (df.iloc[-2]['开盘'] == df.iloc[-2]['收盘'] == df.iloc[-2]['最高'])
        
        # 3. 检查当前是否为“首阴” (放量且收盘低于开盘)
        is_yin = last_row['收盘'] < last_row['开盘']
        volume_ratio = last_row['成交量'] / df.iloc[-2]['成交量']
        is_huge_vol = volume_ratio > 1.5

        # 4. 检查缺口是否回补 (阴线最低价 > 前天收盘价)
        gap_protected = last_row['最低'] > df.iloc[-3]['收盘']

        if is_yin and gap_protected:
            # 评分系统
            score = 60
            if is_one_word: score += 20  # 一字板更有力
            if is_huge_vol: score += 10  # 巨量换手充分
            if last_row['换手率'] < 15: score += 10 # 换手不过分夸张

            suggestion = "暂时观察"
            if score >= 85: suggestion = "重点关注：信号极强，一击必中概率大"
            elif score >= 75: suggestion = "分批试错：符合形态，等待缩量反包"
            
            return {
                "代码": code,
                "名称": names_df.get(code, "未知"),
                "当前价格": curr_price,
                "换手率": last_row['换手率'],
                "量比": round(volume_ratio, 2),
                "信号强度": score,
                "操作建议": suggestion,
                "战法要领": "二板后首阴不破位，博弈次日缩量反包"
            }

    except Exception as e:
        return None
    return None

def main():
    stock_files = glob.glob('stock_data/*.csv')
    names_df = pd.read_csv('stock_names.csv').set_index('code')['name'].to_dict()
    
    # 并行处理提高效率
    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_stock, f, names_df) for f in stock_files]
        for future in futures:
            res = future.result()
            if res:
                results.append(res)
    
    if results:
        output_df = pd.DataFrame(results).sort_values(by="信号强度", ascending=False)
        
        # 创建年月目录
        dir_name = datetime.now().strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = f"{dir_name}/dragon_return_{timestamp}.csv"
        
        output_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"筛选完成，结果已保存至: {file_path}")
    else:
        print("今日无符合战法信号的股票。")

if __name__ == "__main__":
    main()
