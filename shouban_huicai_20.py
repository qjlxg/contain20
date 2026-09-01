import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：首板回踩20日线 (龙回头简易版)
# 逻辑要领：
# 1. 低位放量首板确认主力介入。
# 2. 随后缩量回调，不破20日均线（生命线）。
# 3. 企稳即买点，博弈第二波起浪。
# ==========================================

DATA_DIR = "./stock_data"
NAMES_FILE = "stock_names.csv"
OUTPUT_BASE = "shouban_huicai_20"

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 基础数据清洗
        code = df['股票代码'].iloc[-1].split('.')[-1] # 提取纯数字代码
        code_full = str(df['股票代码'].iloc[-1])
        
        # 1. 基础过滤：排除ST(假设数据中含ST字样或特殊处理), 排除创业板(30开头)
        if code.startswith('30'): return None
        
        # 2. 价格过滤：最新收盘价在 5.0 - 20.0 元之间
        last_close = df['收盘'].iloc[-1]
        if not (5.0 <= last_close <= 20.0): return None
        
        # 3. 计算指标
        df['MA20'] = df['收盘'].rolling(window=20).mean()
        df['Vol_MA5'] = df['成交量'].rolling(window=5).mean()
        
        # 4. 寻找最近 10 天内的首板 (涨幅 > 9.8%)
        recent_10 = df.tail(10).copy()
        limit_ups = recent_10[recent_10['涨跌幅'] >= 9.8]
        
        if limit_ups.empty: return None
        
        # 找到最近的一个涨停日
        last_limit_up_idx = limit_ups.index[-1]
        days_since_limit = (len(df) - 1) - last_limit_up_idx
        
        # 涨停后必须有回调（至少过了一天），且回调天数不宜过长（比如3-8天内最佳）
        if days_since_limit < 1: return None

        # 5. 回踩逻辑判断
        current_ma20 = df['MA20'].iloc[-1]
        vol_limit_up = df['成交量'].loc[last_limit_up_idx]
        current_vol = df['成交量'].iloc[-1]
        
        # 条件 A: 当前收盘价在 MA20 附近 (1% - 3% 范围内)
        is_near_ma20 = 0.98 <= (last_close / current_ma20) <= 1.03
        
        # 条件 B: 缩量（当前成交量小于涨停当日成交量的 60%）
        is_vol_shrink = current_vol < (vol_limit_up * 0.6)
        
        if is_near_ma20 and is_vol_shrink:
            # 6. 打分系统与复盘建议
            score = 0
            if last_close > current_ma20: score += 40  # 线上企稳分高
            if current_vol < df['Vol_MA5'].iloc[-1]: score += 30 # 极度缩量分高
            if df['涨跌幅'].iloc[-1] > -1: score += 30 # 当日收阳或微跌分高
            
            # 建议逻辑
            advice = "试错观察"
            if score >= 80: advice = "重点关注/轻仓介入"
            if score >= 90: advice = "优选/重仓博弈回升"

            return {
                "code": code_full,
                "price": last_close,
                "pct_chg": df['涨跌幅'].iloc[-1],
                "score": score,
                "limit_up_days_ago": days_since_limit,
                "advice": advice
            }
            
    except Exception as e:
        return None
    return None

def main():
    # 1. 扫描文件并行处理
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    pool = mp.Pool(mp.cpu_count())
    results = pool.map(analyze_stock, files)
    pool.close()
    pool.join()
    
    # 2. 过滤结果并匹配名称
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        print("今日无符合战法个股")
        return

    res_df = pd.DataFrame(valid_results)
    
    # 读取名称库
    if os.path.exists(NAMES_FILE):
        names_df = pd.read_csv(NAMES_FILE, dtype={'code': str})
        # 转换代码格式以匹配
        res_df['code_short'] = res_df['code'].apply(lambda x: x.split('.')[-1] if '.' in x else x)
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        res_df = pd.merge(res_df, names_df, left_on='code_short', right_on='code', how='left')
    
    # 3. 优中选优：按分数排序，只取前 5 名 (一击必中原则)
    res_df = res_df.sort_values(by='score', ascending=False).head(5)
    
    # 4. 格式化输出
    final_output = res_df[['code_x', 'name', 'price', 'score', 'advice']].rename(
        columns={'code_x': '代码', 'name': '名称', 'price': '当前价', 'score': '战法评分', 'advice': '操作建议'}
    )
    
    # 5. 保存结果到年月目录
    now = datetime.now()
    dir_path = now.strftime("%Y-%m")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    filename = f"{OUTPUT_BASE}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, filename)
    final_output.to_csv(save_path, index=False, encoding='utf_8_sig')
    
    print(f"分析完成，结果已保存至: {save_path}")

if __name__ == "__main__":
    main()
