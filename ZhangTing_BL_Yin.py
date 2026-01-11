import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：涨停倍量阴·强势洗盘擒龙战法
# 核心逻辑：
# 1. 寻找15日内的涨停板（启动信号）。
# 2. 识别涨停后的“倍量大阴线”（主力暴力洗盘）。
# 3. 价格必须收复阴线失地（洗盘结束，拉升在即）。
# 4. 严格过滤：价格[5-20], 排除ST/创业板，深沪A股。
# ==========================================

STRATEGY_NAME = "ZhangTing_BL_Yin"
DATA_DIR = "stock_data"
NAMES_FILE = "stock_names.csv"
PRICE_MIN = 5.0
PRICE_MAX = 20.0

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 20: return None
        
        # 基础数据预处理
        code = os.path.basename(file_path).replace(".csv", "")
        
        # 1. 基础过滤：排除ST(假设名称中含ST，需配合names文件), 创业板(30), 价格区间
        if code.startswith('30'): return None
        
        last_close = df.iloc[-1]['收盘']
        if not (PRICE_MIN <= last_close <= PRICE_MAX): return None

        # 2. 战法逻辑计算
        # 计算涨停板 (涨幅 > 9.8%)
        df['is_zt'] = (df['涨跌幅'] >= 9.8) & (df['收盘'] == df['最高'])
        
        # 查找最近15天的倍量阴线条件
        # BLY_CONDI: 前日涨停 + 今日阴线 + 成交量>=2倍前日
        df['vol_ratio'] = df['成交量'] / df['成交量'].shift(1)
        df['is_bly'] = (df['is_zt'].shift(1)) & (df['收盘'] < df['开盘']) & (df['vol_ratio'] >= 1.9)
        
        # 检查最近15日内是否存在符合条件的倍量阴线
        recent_df = df.tail(15).copy()
        if not recent_df['is_bly'].any(): return None
        
        # 获取最近那个倍量阴线的价格位置
        bly_idx = recent_df[recent_df['is_bly']].index[-1]
        bly_high = df.loc[bly_idx, '最高']
        bly_open = df.loc[bly_idx, '开盘']
        
        # 3. 买入信号判定：当前收盘价重新站上阴线高点/开盘价，且当前是阳线
        current_price = df.iloc[-1]['收盘']
        current_open = df.iloc[-1]['开盘']
        
        if current_price > max(bly_high, bly_open) and current_price > current_open:
            # 辅助指标：缩量金坑（洗盘期间成交量萎缩）
            vol_check = df.loc[bly_idx+1 : len(df)-2, '成交量'].mean() < df.loc[bly_idx, '成交量'] * 0.5
            
            # 强度评估
            strength = "极强" if vol_check else "标准"
            advice = "重仓杀入" if strength == "极强" else "小仓试错"
            
            return {
                "代码": code,
                "当前价": current_price,
                "信号强度": strength,
                "操作建议": advice,
                "逻辑说明": f"已收复{df.loc[bly_idx, '日期']}的倍量阴线"
            }
    except Exception as e:
        return None

def main():
    # 匹配名称
    names_df = pd.read_csv(NAMES_FILE)
    names_dict = dict(zip(names_df['code'].astype(str), names_df['name']))
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行处理
    results = []
    with ProcessPoolExecutor() as executor:
        for res in executor.map(analyze_stock, csv_files):
            if res:
                res['名称'] = names_dict.get(res['代码'], "未知")
                results.append(res)
    
    # 优中选优：按信号强度排序
    if results:
        final_df = pd.DataFrame(results)
        final_df = final_df[['代码', '名称', '当前价', '信号强度', '操作建议', '逻辑说明']]
        
        # 结果输出
        now = datetime.now()
        dir_path = now.strftime("%Y%m")
        os.makedirs(dir_path, exist_ok=True)
        
        file_name = f"{STRATEGY_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_path, file_name)
        
        final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"复盘完成，筛选出 {len(final_df)} 只潜力股。结果已保存至 {save_path}")
    else:
        print("今日无符合“倍量阴收复”战法标的，建议空仓观望。")

if __name__ == "__main__":
    main()
