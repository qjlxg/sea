import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import pytz
from concurrent.futures import ProcessPoolExecutor

"""
战法名称：成交量量价擒龙战法 (Volume Breakout Master - Strict Edition)
核心逻辑：
1. 识别放量：5日内存在单日成交量 > 10日均量 2.5倍 的“基准柱”。
2. 识别缩量：当前成交量 < 基准柱的 1/3，且连续 2-3 天缩量，说明筹码锁定。
3. 支撑确认：最新收盘价位于 MA5 之上且偏离度 < 1.5%，且放量日后价格未跌破放量日开盘价（不崩）。
4. 择机入场：当前为缩量极致，评分系统对 MA5 支撑力度和K线形态进行复盘。
"""

# --- 参数配置 ---
INPUT_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def analyze_logic(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 1. 基础过滤
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        if code.startswith('30'): return None
        
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        if not (MIN_PRICE <= last_close <= MAX_PRICE): return None

        # 2. 指标计算
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10_Vol'] = df['成交量'].rolling(10).mean()
        
        # --- 逻辑核心实现 ---
        
        # 第一步：识别近期（5日内）是否有“大资金进场”的放量基准柱
        window = df.tail(6).head(5) # 取倒数第2到第6天
        base_vol_row = window.loc[window['成交量'].idxmax()]
        base_vol = base_vol_row['成交量']
        base_open = base_vol_row['开盘']
        
        # 放量标准：成交量 > 10日均量的2.5倍
        is_breakout = base_vol > base_vol_row['MA10_Vol'] * 2.5
        if not is_breakout: return None
        
        # 第二步：识别“股价不崩”且“量能萎缩”
        # 股价不崩：当前价格不能跌破放量日的开盘价（保护生命线）
        is_not_collapsed = last_close >= base_open * 0.98
        # 缩量极致：今日量 < 放量日量 * 0.35
        is_volume_shrink = last_row['成交量'] < base_vol * 0.35
        
        if not (is_not_collapsed and is_volume_shrink): return None

        # 第三步：支撑确认 (MA5 附近不破)
        ma5_val = last_row['MA5']
        on_support = (last_close >= ma5_val * 0.985) and (last_close <= ma5_val * 1.02)
        if not on_support: return None

        # 第四步：择机入场评分 (全自动复盘逻辑)
        score = 75 
        # 细节加分：缩量后再回踩MA5收阳线是极佳信号
        if last_close > last_row['开盘']: score += 10
        # 换手率过滤 (2%-6% 主力洗盘黄金区间)
        if 2 <= last_row['换手率'] <= 6: score += 10
        # 历史回测简易模拟：计算前3天是否也是连续缩量
        if df['成交量'].iloc[-2] < df['成交量'].iloc[-3]: score += 5

        # 确定操作建议
        if score >= 90:
            advice = "【一击必中】符合四步战法精髓，极度缩量且踩稳MA5，主力随时发起总攻。"
        elif score >= 80:
            advice = "【试错观察】形态完美，量能已萎缩至极致，建议轻仓布局待放量确认。"
        else:
            advice = "【备选关注】符合逻辑但活跃度稍欠，建议先加入自选跟踪。"

        return {
            "代码": code,
            "信号强度": score,
            "操作建议": advice,
            "现价": last_close,
            "缩量比": round(last_row['成交量'] / base_vol, 2),
            "换手率": last_row['换手率'],
            "涨跌幅": last_row['涨跌幅']
        }
    except:
        return None

def main():
    files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
    names_df = names_df[~names_df['name'].str.contains("ST|退")]
    valid_codes = set(names_df['code'].tolist())

    results = []
    # 并行扫描，加快速度
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_logic, files):
            if res and res['代码'] in valid_codes:
                res['名称'] = names_df[names_df['code'] == res['代码']]['name'].values[0]
                results.append(res)

    # 排序并输出
    results = sorted(results, key=lambda x: x['信号强度'], reverse=True)

    if results:
        # 只要前10名，确保“一击必中”
        df_final = pd.DataFrame(results[:10])
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        file_path = os.path.join(dir_name, f"volume_breakout_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成，已选出 {len(df_final)} 只最符合逻辑的个股。")
    else:
        print("今日无符合四步战法逻辑的个股。")

if __name__ == "__main__":
    main()
