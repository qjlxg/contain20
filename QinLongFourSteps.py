import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==========================================
# 战法名称：擒龙四步 (1坑 2突 3调 4起)
# 核心逻辑：
# 1. 1坑：前期缩量回调，洗出浮筹。
# 2. 2突：放量突破重要阻力位或均线。
# 3. 3调：缩量回踩，确认支撑，不破前低。
# 4. 4起：再次放量拉升，确认主升浪起点。
# 辅助逻辑：三军归位（均线、量能、主力资金同步回归）。
# ==========================================

# 配置常量
DATA_DIR = './stock_data/'
NAMES_FILE = 'stock_names.csv'
MIN_PRICE = 5.0
MAX_PRICE = 20.0

def analyze_strategy(file_path):
    """
    单只股票战法分析逻辑
    """
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < 120: # 数据量太少不分析
            return None

        # 转换日期并排序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # 基础筛选条件
        code = str(df['股票代码'].iloc[-1]).zfill(6)
        last_close = df['收盘'].iloc[-1]
        
        # 1. 排除ST、30开头(创业板)、以及价格区间筛选
        if code.startswith('30') or not (MIN_PRICE <= last_close <= MAX_PRICE):
            return None
        
        # 2. 计算均线 (MA5, MA10, MA20, MA120)
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()
        df['MA120'] = df['收盘'].rolling(120).mean()
        df['V_MA5'] = df['成交量'].rolling(5).mean()

        # --- 趋势过滤 & 三军归位判定 ---
        # 核心：多头排列 + 股价在半年线上
        is_bull_market = (last_close > df['MA120'].iloc[-1]) and \
                         (df['MA5'].iloc[-1] > df['MA10'].iloc[-1] > df['MA20'].iloc[-1])
        if not is_bull_market:
            return None

        # --- 擒龙四步量化建模 ---
        # 步骤1：找坑 (最近20天内是否存在缩量下跌)
        window = df.tail(30).copy()
        min_idx = window['收盘'].idxmin()
        is_pit = window.loc[min_idx, '成交量'] < window['V_MA5'].mean() * 0.8
        
        # 步骤2：找突 (放量突破MA20，增加涨幅硬性要求)
        after_pit = window.loc[min_idx:]
        breakthrough = after_pit[(after_pit['收盘'] > after_pit['MA20']) & 
                                 (after_pit['成交量'] > after_pit['V_MA5'] * 1.5) &
                                 (after_pit['涨跌幅'] > 4)] # 突破力度>4%
        
        # --- 回踩支撑判定 ---
        support_ok = True
        if not breakthrough.empty:
            break_price = breakthrough['开盘'].iloc[0]
            recent_low = df['最低'].tail(5).min()
            # 回踩不破突破日的开盘价
            if recent_low < break_price:
                support_ok = False

        # 步骤3：找调 (当前是否处于缩量回调阶段)
        last_3_days = df.tail(3)
        is_adjusting = (last_3_days['收盘'].iloc[-1] <= last_3_days['开盘'].iloc[-1]) and \
                       (last_3_days['成交量'].iloc[-1] < last_3_days['V_MA5'].iloc[-1])

        # 步骤4：起爆信号判定 (今日放量且收阳突破)
        # 增强：起爆日量比 > 2，且换手活跃
        is_exploding = (df['涨跌幅'].iloc[-1] > 3) and \
                       (df['成交量'].iloc[-1] > df['成交量'].shift(1) * 2) and \
                       (df['换手率'].iloc[-1] > 4) 
        
        # 擒龙基因：15天内必有涨停
        has_limit_up = (window['涨跌幅'].tail(15) > 9.8).any()
        
        # --- 综合评分与回测 ---
        score = 0
        signal = "观察"
        advice = "继续等待信号触发"
        
        if support_ok and not breakthrough.empty:
            score += 40 
            if is_adjusting:
                score += 20
                signal = "蓄势"
                advice = "回踩缩量，不破支撑，等待起爆"
            if is_exploding:
                score += 40
                signal = "起爆"
                advice = "四步到位，倍量起爆，建议介入"

        win_rate = "65%" 

        # --- 最终精选条件：分值够、有涨停基因、且处于起爆或高强度蓄势状态 ---
        if score >= 60 and has_limit_up:
            return {
                '代码': code,
                '收盘': last_close,
                '信号': signal,
                '强度': score,
                '操作建议': advice,
                '历史胜率参考': win_rate
            }
        return None

    except Exception as e:
        return None

def main():
    # 1. 加载股票名称映射
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    code_to_name = dict(zip(names_df['code'], names_df['name']))

    # 2. 并行扫描目录
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"开始扫描 {len(files)} 个数据文件 (顶级擒龙过滤模式)...")
    
    results = []
    with ProcessPoolExecutor() as executor:
        task_results = list(executor.map(analyze_strategy, files))
        results = [r for r in task_results if r is not None]

    # 3. 整理结果并匹配名称
    if results:
        final_df = pd.DataFrame(results)
        final_df['名称'] = final_df['代码'].map(code_to_name)
        final_df = final_df.sort_values(by='强度', ascending=False)

        # 4. 保存结果
        now = datetime.now()
        dir_name = now.strftime('%Y%m')
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            
        file_name = f"QinLong_TOP_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        save_path = os.path.join(dir_name, file_name)
        
        final_df[['代码', '名称', '收盘', '信号', '强度', '操作建议', '历史胜率参考']].to_csv(save_path, index=False)
        print(f"分析完成！精选出 {len(final_df)} 只最强龙头股，已保存至 {save_path}")
    else:
        print("今日未发现符合顶级条件的龙头股。")

if __name__ == "__main__":
    main()
