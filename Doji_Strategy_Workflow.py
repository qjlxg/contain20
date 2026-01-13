import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ============================================================
# 战法名称：【龙回头十字星潜伏 - 2.0 强化版】
# 逻辑要领：
# 1. 基因：5日内必有涨停，代表主力吸筹完成。
# 2. 洗盘：连续缩量回踩，不破5/10日均线。
# 3. 信号：今日收缩量十字星（假阴真阳或缩量十字），暗示空头衰竭。
# 4. 优化：加入历史回测胜率过滤，只选历史表现最稳的个股。
# ============================================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = './results'

def calculate_strategy(df, i):
    """单票逻辑判断"""
    if i < 20: return False, 0
    
    curr = df.iloc[i]
    prev = df.iloc[i-1]
    
    # 基础过滤
    if not (5.0 <= curr['收盘'] <= 20.0): return False, 0
    
    # 1. 涨停基因：5天内出现过涨停（>9.8%）
    recent = df.iloc[i-5:i]
    limit_up_day = recent[recent['涨跌幅'] > 9.8]
    if limit_up_day.empty: return False, 0
    
    # 2. 缩量回踩：今日成交量 < 5日均量 * 0.8 (严苛缩量)
    ma5 = df['收盘'].rolling(5).mean().iloc[i]
    ma10 = df['收盘'].rolling(10).mean().iloc[i]
    vol_ma5 = df['成交量'].rolling(5).mean().iloc[i]
    
    if curr['成交量'] >= vol_ma5 * 0.8: return False, 0
    
    # 3. 均线支撑：距离MA5或MA10极近（<1%）
    dist_ma5 = abs(curr['收盘'] - ma5) / ma5
    dist_ma10 = abs(curr['收盘'] - ma10) / ma10
    if dist_ma5 > 0.012 and dist_ma10 > 0.012: return False, 0
    
    # 4. 十字星形态：实体极小
    body_pct = abs(curr['收盘'] - curr['开盘']) / curr['开盘']
    if body_pct >= 0.005: return False, 0

    # 评分逻辑
    score = 70
    if curr['收盘'] < curr['开盘']: score += 10 # 阴十字洗盘效果更佳
    if curr['成交量'] < prev['成交量'] * 0.6: score += 20 # 极度枯竭量
    
    return True, score

def analyze_and_backtest(file_path):
    try:
        df = pd.read_csv(file_path)
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除创业板、科创板、ST
        if code.startswith(('30', '688', 'sz4', 'sh4', '4')) or len(df) < 40: return None
        
        # 今日信号
        is_hit, score = calculate_strategy(df, len(df)-1)
        
        # 回测过去120天的表现
        history_wins = []
        for i in range(len(df)-120, len(df)-5):
            hit, _ = calculate_strategy(df, i)
            if hit:
                p_buy = df.iloc[i]['收盘']
                # 统计之后3天的最高价
                p_max = df.iloc[i+1:i+4]['最高'].max()
                history_wins.append(1 if (p_max - p_buy)/p_buy > 0.04 else 0) # 4%算达标
        
        win_rate = np.mean(history_wins) if history_wins else 0

        if is_hit and win_rate >= 0.5: # 历史达标率低于50%的不出票
            suggestion = "【重点加仓】" if score >= 90 else "【小注试错】"
            return {
                '代码': code,
                '当前价': df.iloc[-1]['收盘'],
                '评分': score,
                '历史回测胜率': f"{win_rate*100:.1f}%",
                '信号强度': "极强" if score >= 90 else "标准",
                '操作建议': suggestion + "：回踩到位，关注次日放量上攻"
            }
    except:
        return None

def main():
    # 读取股票名称
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
        # 排除包含ST字样的名称
        final_df = final_df[~final_df['名称'].str.contains('ST')]
        # 选最优的3-5只
        final_df = final_df.sort_values(by=['评分', '历史回测胜率'], ascending=False).head(5)
    else:
        final_df = pd.DataFrame(columns=['代码', '名称', '评分', '历史回测胜率', '操作建议'])

    # 存储结果
    now = datetime.now()
    month_path = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(month_path, exist_ok=True)
    filename = f"Doji_Strategy_Workflow_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    
    final_df.to_csv(os.path.join(month_path, filename), index=False, encoding='utf-8-sig')
    print(f"复盘完成。{now.strftime('%Y-%m-%d')} 筛选出 {len(final_df)} 只符合龙回头逻辑的精品。")

if __name__ == '__main__':
    main()
