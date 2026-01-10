import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# --- 战法配置 ---
STRATEGY_NAME = "倍量过左峰 + RSI辅助"
# 战法要领：
# 1. 寻找前期局部高点（左峰）
# 2. 今日放量（成交量 > 昨量 * 1.9）且收盘价突破左峰
# 3. 辅助指标：RSI(6) > 50 且 RSI(6) > RSI(12) (确保动能向上)
# 4. 价格在 5-20 元之间，排除创业板、科创板、北交所和ST

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def calculate_rsi(series, period=6):
    """计算RSI指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def is_valid_stock(code):
    """筛选：沪深A股，排除创业板、科创板、北交所、ST"""
    if code.startswith('300') or code.startswith('688'): return False 
    if code.startswith('4') or code.startswith('8'): return False 
    return True

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 40: return None # 确保有足够数据计算RSI
        
        # 转换列名确保匹配
        df.columns = [c.strip() for c in df.columns]
        code = os.path.basename(file_path).replace('.csv', '')
        
        if not is_valid_stock(code): return None
        
        # --- 计算技术指标 ---
        # 计算 RSI (6, 12)
        df['rsi_6'] = calculate_rsi(df['收盘'], 6)
        df['rsi_12'] = calculate_rsi(df['收盘'], 12)
        
        # 获取最新数据
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 基础筛选：价格 5-20 元
        curr_price = latest['收盘']
        if not (5.0 <= curr_price <= 25.0): return None # 稍微放宽到25元
        
        # 2. 计算左峰（过去5到20天的最高价）
        recent_window = df.iloc[-21:-1]
        left_peak = recent_window['最高'].max()
        
        # 3. 核心战法逻辑判定
        is_breakout = curr_price > left_peak      # 价格突破左峰
        is_double_vol = latest['成交量'] >= (prev['成交量'] * 1.9) # 近似倍量
        
        # 4. RSI 过滤逻辑
        # RSI6 在 50-85 之间（强势且未到极度超买），且 RSI6 > RSI12 (金叉/多头)
        is_rsi_strong = (latest['rsi_6'] > 50) and (latest['rsi_6'] > latest['rsi_12'])
        
        if is_breakout and is_double_vol and is_rsi_strong:
            # 强度评分
            strength = 70
            if latest['涨跌幅'] > 7: strength += 20 # 强力突破加分
            if latest['rsi_6'] > 80: strength -= 10 # 过于超买适当减分预防冲高回落
            
            suggestion = "观察"
            if strength >= 90: suggestion = "一击必中：动能极强，建议介入"
            elif strength >= 70: suggestion = "试错观察：多头趋势，轻仓跟进"
            
            return {
                "代码": code,
                "现价": curr_price,
                "左峰价": left_peak,
                "成交量比": round(latest['成交量']/prev['成交量'], 2),
                "RSI6": round(latest['rsi_6'], 2),
                "涨跌幅": latest['涨跌幅'],
                "买入信号强度": f"{strength}%",
                "操作建议": suggestion
            }
    except Exception as e:
        # print(f"Error analyzing {file_path}: {e}")
        return None
    return None

def main():
    print(f"开始运行战法：{STRATEGY_NAME}")
    
    # 加载名称映射
    try:
        names_df = pd.read_csv(NAMES_FILE)
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}
        print("警告：未找到股票名称文件，将仅显示代码。")
    
    # 获取所有CSV文件
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print(f"错误：在 {DATA_DIR} 目录下未找到数据文件。")
        return
    
    # 并行处理
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    # 过滤空结果
    final_list = [r for r in results if r is not None]
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        # 匹配名称
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        # 整理列顺序
        cols = ['代码', '名称', '现价', '左峰价', '成交量比', 'RSI6', '涨跌幅', '买入信号强度', '操作建议']
        res_df = res_df[cols]
        
        # 排序：按强度降序
        res_df = res_df.sort_values(by="买入信号强度", ascending=False)
        
        # 创建输出目录
        now = datetime.now()
        dir_path = now.strftime("%Y-%m")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        # 保存结果
        file_name = f"{dir_path}/Breakout_RSI_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        res_df.to_csv(file_name, index=False, encoding='utf_8_sig')
        print(f"\n筛选完成！")
        print(f"符合‘倍量过左峰+RSI强势’条件的股票共: {len(res_df)} 只")
        print(f"结果已保存至: {file_name}")
        print("\n部分筛选结果预览：")
        print(res_df.head())
    else:
        print("今日未找到符合条件的个股。")

if __name__ == "__main__":
    main()
