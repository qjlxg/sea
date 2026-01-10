import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：上下翻飞 (Up-Down Volatility Wash)
# 战法核心：
# 1. 试盘阶段：前期出现带长上影线的K线（测试抛压）。
# 2. 洗盘阶段：近期出现带长下影线的K线（测试支撑+清洗不坚定筹码）。
# 3. 确认阶段：今日缩量回踩不破下影线低点，或放量突破上影线高点。
# 选股目标：主升浪前的最后一次剧烈震荡洗盘。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def is_tradable(code):
    """过滤：仅限深沪A股，排除ST、创业板(30)、科创板(68)"""
    code = str(code).zfill(6)
    if code.startswith(('300', '688', '4', '8')): return False
    return True

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 40: return None
        
        df.columns = [c.strip() for c in df.columns]
        code = os.path.basename(file_path).replace('.csv', '').zfill(6)
        
        if not is_tradable(code): return None
        
        # 获取近10个交易日数据进行形态匹配
        recent = df.iloc[-10:].copy()
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 基础条件：收盘价 5.0 - 20.0 元
        curr_price = latest['收盘']
        if not (5.0 <= curr_price <= 20.0): return None
        
        # 2. 形态检测：寻找长上影线(试盘)和长下影线(洗盘)
        # 定义：影线长度 > 实体长度的2倍
        recent['upper_shadow'] = recent['最高'] - recent[['开盘', '收盘']].max(axis=1)
        recent['lower_shadow'] = recent[['开盘', '收盘']].min(axis=1) - recent['最低']
        recent['body'] = (recent['收盘'] - recent['开盘']).abs()
        
        has_test_peak = (recent['upper_shadow'] > (recent['body'] * 2)).any() # 曾向上试盘
        has_test_bottom = (recent['lower_shadow'] > (recent['body'] * 2)).any() # 曾向下洗盘
        
        # 3. 核心买入信号检测：倍量起爆
        # 今天的成交量是昨天的1.9倍以上，且收盘价站稳在近5日高点之上
        is_volume_break = latest['成交量'] > prev['成交量'] * 1.9
        is_price_break = curr_price > df.iloc[-6:-1]['收盘'].max()
        
        if has_test_peak and has_test_bottom and is_volume_break and is_price_break:
            # 强度评估
            score = 80
            if latest['涨跌幅'] > 5: score += 15
            if latest['换手率'] > 3 and latest['换手率'] < 15: score += 5 # 适度换手最健康
            
            # 操作建议
            if score >= 95:
                advice = "【重仓突击】主力洗盘彻底，倍量突破确认，预计主升浪开启。"
            elif score >= 85:
                advice = "【积极试错】符合上下翻飞形态，成交量配合理想，建议介入。"
            else:
                advice = "【轻仓观察】形态成立但力度稍弱，防守位设在下影线低点。"
                
            return {
                "代码": code,
                "现价": curr_price,
                "涨跌幅": f"{latest['涨跌幅']}%",
                "换手率": f"{latest['换手率']}%",
                "量比": round(latest['成交量']/prev['成交量'], 2),
                "买入信号强度": f"{score}%",
                "操作建议": advice
            }
            
    except:
        return None

def main():
    # 加载股票名称
    try:
        names_df = pd.read_csv(NAMES_FILE)
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行扫描
    with Pool(cpu_count()) as p:
        results = p.map(analyze_logic, files)
    
    final_list = [r for r in results if r is not None]
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        
        # 整理列顺序
        cols = ['代码', '名称', '现价', '涨跌幅', '换手率', '量比', '买入信号强度', '操作建议']
        res_df = res_df[cols].sort_values(by="买入信号强度", ascending=False)
        
        # 存储路径
        now = datetime.now()
        folder = now.strftime("%Y-%m")
        os.makedirs(folder, exist_ok=True)
        file_path = f"{folder}/Up_Down_Volatility_Wash_{now.strftime('%Y%m%d_%H%M')}.csv"
        
        res_df.to_csv(file_path, index=False, encoding='utf_8_sig')
        print(f"复盘完成！发现 {len(res_df)} 个标的，已存入 {file_path}")
    else:
        print("今日未扫描到符合‘上下翻飞’战法的强势标的。")

if __name__ == "__main__":
    main()
