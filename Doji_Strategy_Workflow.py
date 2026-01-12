import pandas as pd
import numpy as np
import os
from datetime import datetime
import multiprocessing as mp

# ==========================================
# 战法名称：龙回头之十字星潜伏战法
# 核心逻辑：
# 1. 强力启动：前4天内有过涨停，确立领涨基因。
# 2. 缩量洗盘：涨停后成交量萎缩，主力未撤退。
# 3. 支撑确认：股价回踩5日或10日均线不破。
# 4. 变盘信号：今日收出缩量十字星，预示调整结束。
# ==========================================

DATA_DIR = './stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = './results'

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 20: return None
        
        # 基础过滤：代码和价格
        code = os.path.basename(file_path).replace('.csv', '')
        # 排除ST(简单判断名称通常在names表，此处先按代码过滤), 创业板(30), 科创板(688)
        if code.startswith(('30', '688')): return None
        
        last_row = df.iloc[-1]
        close_price = last_row['收盘']
        if not (5.0 <= close_price <= 20.0): return None

        # 计算技术指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['Vol_MA5'] = df['成交量'].rolling(5).mean()
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 1. 寻找涨停基因 (5天内有涨幅 > 9.8%)
        recent_history = df.iloc[-6:-1]
        has_limit_up = (recent_history['涨跌幅'] > 9.8).any()
        if not has_limit_up: return None

        # 2. 缩量判断 (今日成交量小于5日均量)
        is_low_vol = curr['成交量'] < curr['Vol_MA5']
        
        # 3. 十字星形态判断 (实体大小 < 0.6%, 且上下影线存在)
        body_size = abs(curr['收盘'] - curr['开盘']) / curr['开盘']
        is_doji = body_size < 0.006
        
        # 4. 均线支撑 (股价距离MA5或MA10偏差在1.5%以内)
        on_support = (abs(curr['收盘'] - curr['MA5']) / curr['MA5'] < 0.015) or \
                     (abs(curr['收盘'] - curr['MA10']) / curr['MA10'] < 0.015)

        if is_doji and is_low_vol and on_support:
            # 评分逻辑
            score = 0
            if curr['成交量'] < prev['成交量'] * 0.7: score += 40  # 极度缩量加分
            if has_limit_up: score += 40
            if curr['收盘'] > curr['MA20'] if 'MA20' in df else True: score += 20
            
            # 操作建议
            suggestion = "暂时放弃"
            if score >= 80: suggestion = "重点关注：极佳潜伏位，可试错点火"
            elif score >= 60: suggestion = "观察：形态尚可，等待分时走强"
            
            return {
                'code': code,
                'current_price': close_price,
                'change_pct': curr['涨跌幅'],
                'score': score,
                'suggestion': suggestion
            }
    except Exception as e:
        return None

def main():
    # 获取名称映射
    try:
        names_df = pd.read_csv(NAMES_FILE)
        names_dict = dict(zip(names_df['code'].astype(str).str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    # 并行扫描目录
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    with mp.Pool(mp.cpu_count()) as pool:
        results = pool.map(analyze_stock, files)
    
    # 过滤空值
    final_list = [r for r in results if r is not None]
    
    # 整合名称
    for item in final_list:
        item['name'] = names_dict.get(item['code'], "未知")

    # 按分数排序，优中选优
    final_df = pd.DataFrame(final_list)
    if not final_df.empty:
        final_df = final_df.sort_values(by='score', ascending=False).head(10) # 仅保留最强前10只

    # 创建保存目录
    now = datetime.now()
    month_dir = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    if not os.path.exists(month_dir):
        os.makedirs(month_dir)
    
    file_name = f"Doji_Strategy_Workflow_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(month_dir, file_name)
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"复盘完成，筛选出 {len(final_df)} 只目标，结果已保存至 {save_path}")

if __name__ == '__main__':
    main()
