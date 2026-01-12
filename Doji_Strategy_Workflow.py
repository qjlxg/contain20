import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法：【龙回头十字星潜伏】
# 逻辑：涨停基因(5日内) + 缩量回踩(MA5/10) + 十字星变盘信号
# 回测：自动计算历史上该信号出现后的 3-5 日胜率与盈亏
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = './results'

def calculate_strategy(df, i):
    """核心战法逻辑：判断第 i 行是否触发信号"""
    if i < 15: return False, 0
    
    curr = df.iloc[i]
    prev = df.iloc[i-1]
    
    # 1. 价格过滤 (5-20元)
    if not (5.0 <= curr['收盘'] <= 20.0): return False, 0
    
    # 2. 涨停基因 (前5天内有涨停)
    recent = df.iloc[i-5:i]
    if not (recent['涨跌幅'] > 9.8).any(): return False, 0
    
    # 3. 十字星 (实体 < 0.6%)
    body_size = abs(curr['收盘'] - curr['开盘']) / curr['开盘']
    if body_size >= 0.006: return False, 0
    
    # 4. 缩量回踩 (缩量且接近MA5/MA10)
    ma5 = df['收盘'].rolling(5).mean().iloc[i]
    ma10 = df['收盘'].rolling(10).mean().iloc[i]
    vol_ma5 = df['成交量'].rolling(5).mean().iloc[i]
    
    on_support = (abs(curr['收盘'] - ma5)/ma5 < 0.015) or (abs(curr['收盘'] - ma10)/ma10 < 0.015)
    is_low_vol = curr['成交量'] < vol_ma5
    
    if on_support and is_low_vol:
        # 计算评分
        score = 60
        if curr['成交量'] < prev['成交量'] * 0.7: score += 20
        if curr['涨跌幅'] < 0: score += 20 # 绿星洗盘更佳
        return True, score
    
    return False, 0

def analyze_and_backtest(file_path):
    try:
        df = pd.read_csv(file_path)
        code = os.path.basename(file_path).replace('.csv', '')
        if code.startswith(('30', '688')) or len(df) < 30: return None
        
        # --- 1. 今日实时筛选 ---
        is_hit, score = calculate_strategy(df, len(df)-1)
        
        # --- 2. 历史回测 (扫描过去60个交易日) ---
        hits = []
        for i in range(len(df)-60, len(df)-5): # 留出5天看结果
            hit, s = calculate_strategy(df, i)
            if hit:
                # 计算5日后最高收益
                p_now = df.iloc[i]['收盘']
                p_future = df.iloc[i+1:i+6]['最高'].max()
                profit = (p_future - p_now) / p_now * 100
                hits.append(profit)
        
        win_rate = f"{len([h for h in hits if h > 3]) / len(hits) * 100:.1f}%" if hits else "无数据"
        avg_profit = f"{np.mean(hits):.2f}%" if hits else "N/A"

        if is_hit:
            suggestion = "暂时放弃"
            if score >= 90: suggestion = "【一击必中】极度缩量+强支撑，重仓伏击"
            elif score >= 80: suggestion = "【试错】形态标准，分时走强可买"
            elif score >= 60: suggestion = "【观察】等缩量更极致或回踩不破"
            
            return {
                '代码': code,
                '当前价': df.iloc[-1]['收盘'],
                '战法评分': score,
                '历史回测胜率(>3%)': win_rate,
                '历史次5日均收益': avg_profit,
                '操作建议': suggestion
            }
    except:
        return None

def main():
    try:
        names_df = pd.read_csv(NAMES_FILE)
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    with mp.Pool(mp.cpu_count()) as pool:
        results = [r for r in pool.map(analyze_and_backtest, files) if r is not None]
    
    if results:
        final_df = pd.DataFrame(results)
        final_df['名称'] = final_df['代码'].apply(lambda x: names_dict.get(x, "未知"))
        # 排序：评分高 -> 胜率高
        final_df = final_df.sort_values(by=['战法评分'], ascending=False).head(5) # 极其严选前5只
    else:
        final_df = pd.DataFrame(columns=['代码', '名称', '战法评分', '操作建议'])

    # 保存路径
    now = datetime.now()
    month_str = now.strftime('%Y%m')
    os.makedirs(os.path.join(OUTPUT_BASE, month_str), exist_ok=True)
    save_path = f"{OUTPUT_BASE}/{month_str}/Doji_Strategy_Workflow_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"筛选及回测完成。精选目标：{len(final_df)} 只。")

if __name__ == '__main__':
    main()
