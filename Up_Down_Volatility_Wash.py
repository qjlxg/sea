import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：上下翻飞 (极致精选回测版)
# 战法要领：
# 1. 试盘与洗盘：10日内必须出现长上影(试盘)和长下影(震仓)，影线 > 实体1.8倍。
# 2. 动能确认：RSI(14) 在 50-75 强势区间，拒绝弱势股与超买股。
# 3. 资金门槛：换手率 3%-12%，价格 5-20元，排除ST、创业、科创。
# 4. 胜率优选：自动回测该股历史同类形态后5日表现，只做“惯性上涨”股。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"

def calculate_rsi(series, period=14):
    """计算RSI指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def get_historical_win_rate(df):
    """历史回测逻辑：回溯过去一年出现相似量价特征后的平均最高涨幅"""
    if len(df) < 120: return 0
    profits = []
    # 模拟历史扫描（简化特征匹配以提高速度）
    for i in range(20, len(df) - 6):
        prev_vol = df.iloc[i-1]['成交量']
        curr_vol = df.iloc[i]['成交量']
        # 匹配放量突破特征
        if curr_vol > prev_vol * 1.8 and df.iloc[i]['涨跌幅'] > 2:
            entry_price = df.iloc[i]['收盘']
            max_p = df.iloc[i+1 : i+6]['最高'].max()
            profits.append((max_p - entry_price) / entry_price * 100)
    return np.mean(profits) if profits else 0

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        if len(df) < 60: return None
        
        # 提取代码并硬性过滤
        code = os.path.basename(file_path).replace('.csv', '').zfill(6)
        if code.startswith(('30', '68', '4', '8', '9')): return None # 排除创业、科创、北交

        # 基础指标计算
        df['rsi'] = calculate_rsi(df['收盘'], 14)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 基础硬性条件：价格、换手、RSI
        if not (5.0 <= latest['收盘'] <= 20.0): return None
        if not (3.0 <= latest['换手率'] <= 12.0): return None
        if not (50 <= latest['rsi'] <= 75): return None

        # 2. 上下翻飞形态识别 (近10日窗口)
        window = df.iloc[-10:].copy()
        window['u_shadow'] = window['最高'] - window[['开盘', '收盘']].max(axis=1)
        window['l_shadow'] = window[['开盘', '收盘']].min(axis=1) - window['最低']
        window['body'] = (window['收盘'] - window['开盘']).abs().replace(0, 0.01)
        
        has_up = (window['u_shadow'] > window['body'] * 1.8).any() 
        has_down = (window['l_shadow'] > window['body'] * 1.8).any()
        if not (has_up and has_down): return None

        # 3. 核心触发逻辑：今日倍量突破 或 缩量止跌
        is_breakout = (latest['收盘'] > window['收盘'].max() * 0.98) and (latest['成交量'] > prev['成交量'] * 1.8)
        is_wash = (latest['成交量'] < prev['成交量'] * 0.6) and (abs(latest['涨跌幅']) < 2.5)
        
        if not (is_breakout or is_wash): return None

        # 4. 历史胜率评分
        avg_profit = get_historical_win_rate(df)
        
        # 5. 最终权重评分
        score = 70
        if is_breakout: score += 15
        if avg_profit > 4: score += 10
        if latest['振幅'] > 4: score += 5
        
        # 宁缺毋滥：只有高分进入结果
        if score < 85: return None

        # 6. 生成全自动复盘文字
        signal_type = "【倍量起爆】" if is_breakout else "【缩量洗盘】"
        suggestion = "一击必中：建议次日结合分时图择机切入。" if score >= 95 else "精选观察：回踩支撑位不破可试错。"

        return {
            "代码": code,
            "现价": latest['收盘'],
            "涨跌幅": f"{latest['涨跌幅']}%",
            "换手率": f"{latest['换手率']}%",
            "RSI14": round(latest['rsi'], 2),
            "历史期望": f"{round(avg_profit, 2)}%",
            "信号强度": f"{score}%",
            "全自动复盘逻辑": f"{signal_type} {suggestion}"
        }
    except:
        return None

def main():
    # 匹配名称并排除ST
    try:
        names_df = pd.read_csv(NAMES_FILE)
        # 排除ST及退市股
        names_df = names_df[~names_df['name'].str.contains("ST|退", na=False)]
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行加速处理
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, files)
    
    final_list = [r for r in results if r is not None and r['代码'] in names_dict]
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        res_df['名称'] = res_df['代码'].apply(lambda x: names_dict.get(x))
        
        # 结果保存至年月文件夹
        now = datetime.now()
        out_dir = now.strftime("%Y-%m")
        os.makedirs(out_dir, exist_ok=True)
        file_name = f"{out_dir}/Up_Down_Volatility_Wash_{now.strftime('%Y%m%d_%H%M')}.csv"
        
        cols = ['代码', '名称', '现价', '涨跌幅', '换手率', 'RSI14', '历史期望', '信号强度', '全自动复盘逻辑']
        res_df[cols].sort_values(by="信号强度", ascending=False).to_csv(file_name, index=False, encoding='utf_8_sig')
        print(f"筛选完成！优化后共找到 {len(res_df)} 只高价值个股。")
    else:
        print("今日无符合顶格条件的个股，保持空仓也是一种战术。")

if __name__ == "__main__":
    main()
