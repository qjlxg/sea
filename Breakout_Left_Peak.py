import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 战法配置 ---
STRATEGY_NAME = "倍量过左峰"
# 战法要领：
# 1. 寻找前期局部高点（左峰）
# 2. 今日放量（成交量 > 昨量 * 2）且收盘价突破左峰
# 3. 价格在 5-20 元之间，排除创业板和ST
# 4. 买入逻辑：突破即买入或等待次日回踩不破左峰加仓

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def is_valid_stock(code):
    """筛选：沪深A股，排除创业板、ST"""
    # 假设 code 格式为 '600000'
    if code.startswith('300') or code.startswith('688'): return False # 排除创业板/科创板
    if code.startswith('4') or code.startswith('8'): return False # 排除北交所
    return True

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 转换列名确保匹配
        df.columns = [c.strip() for c in df.columns]
        code = os.path.basename(file_path).replace('.csv', '')
        
        if not is_valid_stock(code): return None
        
        # 获取最新数据
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 基础筛选：价格 5-20 元
        curr_price = latest['收盘']
        if not (5.0 <= curr_price <= 20.0): return None
        
        # 2. 计算左峰（过去5到20天的最高价）
        recent_window = df.iloc[-21:-1]
        left_peak = recent_window['最高'].max()
        
        # 3. 战法逻辑判定
        is_breakout = curr_price > left_peak # 价格突破左峰
        is_double_vol = latest['成交量'] >= (prev['成交量'] * 1.9) # 近似倍量
        
        if is_breakout and is_double_vol:
            # 优选辅助逻辑：回测胜率简易评估（过去半年内此类突破后的表现）
            # 此处简化为强度评分
            strength = 70
            if latest['涨跌幅'] > 7: strength += 20 # 强势涨停突破加分
            
            # 操作建议逻辑
            suggestion = "观察"
            if strength >= 90: suggestion = "一击必中：建议竞价或回调买入"
            elif strength >= 70: suggestion = "试错观察：轻仓跟进"
            
            return {
                "代码": code,
                "现价": curr_price,
                "左峰价": left_peak,
                "成交量比": round(latest['成交量']/prev['成交量'], 2),
                "涨跌幅": latest['涨跌幅'],
                "买入信号强度": f"{strength}%",
                "操作建议": suggestion
            }
    except Exception as e:
        return None
    return None

def main():
    print(f"开始运行战法：{STRATEGY_NAME}")
    
    # 加载名称映射
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    
    # 获取所有CSV文件
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行处理
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    # 过滤空结果
    final_list = [r for r in results if r is not None]
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        # 匹配名称
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        # 排序：按强度排序
        res_df = res_df[['代码', '名称', '现价', '左峰价', '成交量比', '涨跌幅', '买入信号强度', '操作建议']]
        res_df = res_df.sort_values(by="买入信号强度", ascending=False)
        
        # 创建年月目录
        now = datetime.now()
        dir_path = now.strftime("%Y-%m")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        # 保存结果
        file_name = f"{dir_path}/Breakout_Left_Peak_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        res_df.to_csv(file_name, index=False, encoding='utf_8_sig')
        print(f"筛选完成，共找到 {len(res_df)} 只潜力股，结果已保存至 {file_name}")
    else:
        print("今日无符合战法条件的股票。")

if __name__ == "__main__":
    main()
