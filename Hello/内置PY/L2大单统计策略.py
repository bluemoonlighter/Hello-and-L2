# coding:gbk
"""
L2大单统计策略 - 沪深成交额TOP股票池
功能：读取沪市成交额前990只 + 深市成交额前990只 的L2大单统计数据

说明：
1. 股票池：沪市成交额前990只 + 深市成交额前990只 = 共1980只
2. 使用 get_market_data_ex 函数获取L2大单统计数据
3. period参数设置为 'l2transactioncount' 表示L2逐笔成交统计（大单统计）

【资金流向计算规则】
- 特大单(Most): 成交金额>=100万 或 成交量>=5000手
- 大单(Big): 成交金额>=20万 或 成交量>=1000手  
- 中单(Medium): 成交金额>=4万 或 成交量>=200手
- 小单(Small): 其他

【主力净流入计算公式】
主力净流入 = (主买特大单 + 主买大单 + 被动买特大单 + 被动买大单) 
           - (主卖特大单 + 主卖大单 + 被动卖特大单 + 被动卖大单)
"""

import pandas as pd
import numpy as np
import datetime
import time

# 全局变量类，用于保存状态
class GlobalState:
    pass

G = GlobalState()


def init(C):
    """
    初始化函数，策略启动时执行一次
    """
    print("=" * 60)
    print("L2大单统计策略初始化")
    print("=" * 60)
    
    # 股票池将在第一次handlebar中初始化
    G.stock_list = None
    G.stock_pool_initialized = False
    
    # 设置分批处理参数（每批处理的股票数量）
    G.batch_size = 500
    
    # 需要获取的L2大单统计字段
    G.l2_fields = [
        'time',               # 时间戳
        'ddx',                # 大单动向
        'ddy',                # 涨跌动因
        'ddz',                # 大单差分
        
        # === 主买成交量数据 (主动买入) ===
        'bidMostVolume',      # 主买特大单成交量
        'bidBigVolume',       # 主买大单成交量
        'bidMediumVolume',    # 主买中单成交量
        'bidSmallVolume',     # 主买小单成交量
        'bidTotalVolume',     # 主买累计成交量
        
        # === 被动买成交量数据 (被动买入) ===
        'unactiveBidMostVolume',   # 被动买特大单成交量
        'unactiveBidBigVolume',    # 被动买大单成交量
        'unactiveBidMediumVolume', # 被动买中单成交量
        'unactiveBidSmallVolume',  # 被动买小单成交量
        'unactiveBidTotalVolume',  # 被动买累计成交量
        
        # === 主卖成交量数据 (主动卖出) ===
        'offMostVolume',      # 主卖特大单成交量
        'offBigVolume',       # 主卖大单成交量
        'offMediumVolume',    # 主卖中单成交量
        'offSmallVolume',     # 主卖小单成交量
        'offTotalVolume',     # 主卖累计成交量
        
        # === 被动卖成交量数据 (被动卖出) ===
        'unactiveOffMostVolume',   # 被动卖特大单成交量
        'unactiveOffBigVolume',    # 被动卖大单成交量
        'unactiveOffMediumVolume', # 被动卖中单成交量
        'unactiveOffSmallVolume',  # 被动卖小单成交量
        'unactiveOffTotalVolume',  # 被动卖累计成交量
        
        # === 主买成交额数据 ===
        'bidMostAmount',      # 主买特大单成交额
        'bidBigAmount',       # 主买大单成交额
        'bidMediumAmount',    # 主买中单成交额
        'bidSmallAmount',     # 主买小单成交额
        'bidTotalAmount',     # 主买累计成交额
        
        # === 被动买成交额数据 ===
        'unactiveBidMostAmount',   # 被动买特大单成交额
        'unactiveBidBigAmount',    # 被动买大单成交额
        'unactiveBidMediumAmount', # 被动买中单成交额
        'unactiveBidSmallAmount',  # 被动买小单成交额
        'unactiveBidTotalAmount',  # 被动买累计成交额
        
        # === 主卖成交额数据 ===
        'offMostAmount',      # 主卖特大单成交额
        'offBigAmount',       # 主卖大单成交额
        'offMediumAmount',    # 主卖中单成交额
        'offSmallAmount',     # 主卖小单成交额
        'offTotalAmount',     # 主卖累计成交额
        
        # === 被动卖成交额数据 ===
        'unactiveOffMostAmount',   # 被动卖特大单成交额
        'unactiveOffBigAmount',    # 被动卖大单成交额
        'unactiveOffMediumAmount', # 被动卖中单成交额
        'unactiveOffSmallAmount',  # 被动卖小单成交额
        'unactiveOffTotalAmount',  # 被动卖累计成交额
    ]
    
    # 记录上次处理时间（用于控制执行频率）
    G.last_process_time = None
    
    # 保存最新的大单统计数据
    G.latest_l2_data = None
    
    print("【股票池】将在第一次运行时初始化")
    print("【规则】沪市成交额前990只 + 深市成交额前990只")
    print("=" * 60)


def init_stock_pool(C):
    """
    初始化股票池：获取沪市成交额前990 + 深市成交额前990
    """
    print("\n" + "=" * 60)
    print("【初始化股票池】获取成交额TOP股票...")
    print("=" * 60)
    
    # 获取沪深A股全市场
    all_stocks = C.get_stock_list_in_sector('沪深A股')
    print(f"全市场股票总数: {len(all_stocks)}")
    
    # 分离沪市和深市
    sh_stocks = [s for s in all_stocks if s.endswith('.SH')]
    sz_stocks = [s for s in all_stocks if s.endswith('.SZ')]
    print(f"沪市股票数: {len(sh_stocks)}")
    print(f"深市股票数: {len(sz_stocks)}")
    
    # 分批获取成交额数据（使用tick数据获取最新成交额）
    batch_size = 1000
    sh_amounts = []
    sz_amounts = []
    
    # 获取沪市成交额
    print("\n正在获取沪市股票成交额...")
    for i in range(0, len(sh_stocks), batch_size):
        batch = sh_stocks[i:i+batch_size]
        tick_data = C.get_full_tick(batch)
        for code in batch:
            if code in tick_data and 'amount' in tick_data[code]:
                sh_amounts.append((code, tick_data[code]['amount']))
            else:
                sh_amounts.append((code, 0))
        if (i // batch_size + 1) % 5 == 0:
            print(f"  沪市进度: {min(i+batch_size, len(sh_stocks))}/{len(sh_stocks)}")
    
    # 获取深市成交额
    print("\n正在获取深市股票成交额...")
    for i in range(0, len(sz_stocks), batch_size):
        batch = sz_stocks[i:i+batch_size]
        tick_data = C.get_full_tick(batch)
        for code in batch:
            if code in tick_data and 'amount' in tick_data[code]:
                sz_amounts.append((code, tick_data[code]['amount']))
            else:
                sz_amounts.append((code, 0))
        if (i // batch_size + 1) % 5 == 0:
            print(f"  深市进度: {min(i+batch_size, len(sz_stocks))}/{len(sz_stocks)}")
    
    # 按成交额排序，取前990
    sh_amounts.sort(key=lambda x: x[1], reverse=True)
    sz_amounts.sort(key=lambda x: x[1], reverse=True)
    
    top_sh = [code for code, _ in sh_amounts[:990]]
    top_sz = [code for code, _ in sz_amounts[:990]]
    
    # 合并股票池
    G.stock_list = top_sh + top_sz
    G.sh_list = top_sh
    G.sz_list = top_sz
    
    print("\n" + "=" * 60)
    print("【股票池初始化完成】")
    print(f"沪市成交额TOP990: {len(top_sh)} 只")
    print(f"深市成交额TOP990: {len(top_sz)} 只")
    print(f"总计: {len(G.stock_list)} 只")
    print("=" * 60)
    
    # 显示沪市前10
    print("\n沪市成交额TOP10:")
    for code, amount in sh_amounts[:10]:
        name = C.get_stock_name(code)
        print(f"  {code} {name}: {amount/100000000:.2f}亿")
    
    # 显示深市前10
    print("\n深市成交额TOP10:")
    for code, amount in sz_amounts[:10]:
        name = C.get_stock_name(code)
        print(f"  {code} {name}: {amount/100000000:.2f}亿")
    
    G.stock_pool_initialized = True


def get_l2_data_batch(C, stock_list, fields=[]):
    """
    获取一批股票的L2大单统计数据
    """
    try:
        data = C.get_market_data_ex(
            fields=fields,
            stock_code=stock_list,
            period='l2transactioncount',
            count=1,
            subscribe=True
        )
        return data
    except Exception as e:
        print(f"获取L2数据失败: {str(e)}")
        return {}


def safe_get(row, field, default=0):
    """安全获取字段值，处理None和NaN"""
    try:
        value = row.get(field, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value) if isinstance(value, (int, float, np.number)) else default
    except:
        return default


def analyze_l2_data(l2_data):
    """
    分析L2大单统计数据
    """
    results = []
    
    for stock_code, df in l2_data.items():
        if df is None or df.empty:
            continue
            
        try:
            row = df.iloc[-1]
            
            result = {
                'stock_code': stock_code,
                'time': safe_get(row, 'time', 0),
            }
            
            # 获取DDX/DDY/DDZ
            result['ddx'] = safe_get(row, 'ddx', 0)
            result['ddy'] = safe_get(row, 'ddy', 0)
            result['ddz'] = safe_get(row, 'ddz', 0)
            
            # ===== 成交量数据 =====
            bid_most_vol = safe_get(row, 'bidMostVolume', 0)
            bid_big_vol = safe_get(row, 'bidBigVolume', 0)
            unactive_bid_most_vol = safe_get(row, 'unactiveBidMostVolume', 0)
            unactive_bid_big_vol = safe_get(row, 'unactiveBidBigVolume', 0)
            off_most_vol = safe_get(row, 'offMostVolume', 0)
            off_big_vol = safe_get(row, 'offBigVolume', 0)
            unactive_off_most_vol = safe_get(row, 'unactiveOffMostVolume', 0)
            unactive_off_big_vol = safe_get(row, 'unactiveOffBigVolume', 0)
            
            # ===== 成交额数据 =====
            bid_most_amount = safe_get(row, 'bidMostAmount', 0)
            bid_big_amount = safe_get(row, 'bidBigAmount', 0)
            unactive_bid_most_amount = safe_get(row, 'unactiveBidMostAmount', 0)
            unactive_bid_big_amount = safe_get(row, 'unactiveBidBigAmount', 0)
            off_most_amount = safe_get(row, 'offMostAmount', 0)
            off_big_amount = safe_get(row, 'offBigAmount', 0)
            unactive_off_most_amount = safe_get(row, 'unactiveOffMostAmount', 0)
            unactive_off_big_amount = safe_get(row, 'unactiveOffBigAmount', 0)
            
            # ===== 计算主力净流入 =====
            total_buy_most_vol = bid_most_vol + unactive_bid_most_vol
            total_buy_big_vol = bid_big_vol + unactive_bid_big_vol
            total_buy_most_amount = bid_most_amount + unactive_bid_most_amount
            total_buy_big_amount = bid_big_amount + unactive_bid_big_amount
            
            total_sell_most_vol = off_most_vol + unactive_off_most_vol
            total_sell_big_vol = off_big_vol + unactive_off_big_vol
            total_sell_most_amount = off_most_amount + unactive_off_most_amount
            total_sell_big_amount = off_big_amount + unactive_off_big_amount
            
            result['main_net_inflow_volume'] = (total_buy_most_vol + total_buy_big_vol) - (total_sell_most_vol + total_sell_big_vol)
            result['net_inflow_most_volume'] = total_buy_most_vol - total_sell_most_vol
            result['net_inflow_big_volume'] = total_buy_big_vol - total_sell_big_vol
            
            result['main_net_inflow_amount'] = (total_buy_most_amount + total_buy_big_amount) - (total_sell_most_amount + total_sell_big_amount)
            result['net_inflow_most_amount'] = total_buy_most_amount - total_sell_most_amount
            result['net_inflow_big_amount'] = total_buy_big_amount - total_sell_big_amount
            
            # 保存原始数据
            result['bid_most_volume'] = bid_most_vol
            result['unactive_bid_most_volume'] = unactive_bid_most_vol
            result['bid_big_volume'] = bid_big_vol
            result['unactive_bid_big_volume'] = unactive_bid_big_vol
            result['off_most_volume'] = off_most_vol
            result['unactive_off_most_volume'] = unactive_off_most_vol
            result['off_big_volume'] = off_big_vol
            result['unactive_off_big_volume'] = unactive_off_big_vol
            
            bid_total_amount = safe_get(row, 'bidTotalAmount', 0)
            off_total_amount = safe_get(row, 'offTotalAmount', 0)
            result['bid_total_amount'] = bid_total_amount
            result['off_total_amount'] = off_total_amount
            
            if bid_total_amount > 0:
                result['big_buy_ratio'] = (total_buy_most_amount + total_buy_big_amount) / bid_total_amount
            else:
                result['big_buy_ratio'] = 0
                
            if off_total_amount > 0:
                result['big_sell_ratio'] = (total_sell_most_amount + total_sell_big_amount) / off_total_amount
            else:
                result['big_sell_ratio'] = 0
            
            total_amount = bid_total_amount + off_total_amount
            if total_amount > 0:
                result['main_inflow_ratio'] = result['main_net_inflow_amount'] / total_amount
            else:
                result['main_inflow_ratio'] = 0
            
            results.append(result)
            
        except Exception as e:
            continue
    
    if not results:
        return pd.DataFrame()
        
    return pd.DataFrame(results)


def get_all_l2_data(C):
    """
    分批获取L2大单统计数据
    """
    all_data = {}
    total_batches = (len(G.stock_list) + G.batch_size - 1) // G.batch_size
    
    for i in range(total_batches):
        start_idx = i * G.batch_size
        end_idx = min((i + 1) * G.batch_size, len(G.stock_list))
        batch_stocks = G.stock_list[start_idx:end_idx]
        
        batch_data = get_l2_data_batch(C, batch_stocks, G.l2_fields)
        
        if batch_data:
            all_data.update(batch_data)
        
        if (i + 1) % 2 == 0 or i == total_batches - 1:
            print(f"L2数据获取进度: {i + 1}/{total_batches} 批，已获取 {len(all_data)} 只股票数据")
    
    return all_data


def handlebar(C):
    """
    行情事件函数
    """
    # 只在最后一根K线执行
    if not C.is_last_bar():
        return
    
    # 初始化股票池（只在第一次运行时执行）
    if not G.stock_pool_initialized:
        init_stock_pool(C)
    
    # 控制执行频率
    now = datetime.datetime.now()
    if G.last_process_time is not None:
        elapsed = (now - G.last_process_time).total_seconds()
        if elapsed < 5:
            return
    
    G.last_process_time = now
    current_time = now.strftime('%H:%M:%S')
    
    print(f"\n{'=' * 80}")
    print(f"[{current_time}] 开始获取L2大单统计数据")
    print(f"股票池: 沪市成交额TOP990 + 深市成交额TOP990 = {len(G.stock_list)}只")
    print('=' * 80)
    
    start_time = time.time()
    
    l2_data = get_all_l2_data(C)
    
    if not l2_data:
        print("未获取到L2数据")
        return
    
    analysis_df = analyze_l2_data(l2_data)
    
    if analysis_df.empty:
        print("数据分析结果为空")
        return
    
    G.latest_l2_data = analysis_df
    
    elapsed_time = time.time() - start_time
    print(f"\n数据获取和分析完成，耗时: {elapsed_time:.2f}秒")
    
    # 输出分析结果
    print(f"\n{'=' * 80}")
    print("【大单动向 DDX 排名 TOP10】")
    print('=' * 80)
    top_ddx = analysis_df.nlargest(10, 'ddx')[['stock_code', 'ddx', 'ddy', 'main_net_inflow_volume']]
    for idx, row in top_ddx.iterrows():
        stock_name = C.get_stock_name(row['stock_code'])
        print(f"{row['stock_code']} {stock_name:8s} DDX:{row['ddx']:10.2f} DDY:{row['ddy']:10.2f} 主力净流入(量):{row['main_net_inflow_volume']:15.0f}")
    
    print(f"\n{'=' * 80}")
    print("【主力净流入(量) 排名 TOP10】")
    print('=' * 80)
    top_inflow = analysis_df.nlargest(10, 'main_net_inflow_volume')[['stock_code', 'main_net_inflow_volume', 'net_inflow_most_volume', 'net_inflow_big_volume']]
    for idx, row in top_inflow.iterrows():
        stock_name = C.get_stock_name(row['stock_code'])
        print(f"{row['stock_code']} {stock_name:8s} 主力净流入(量):{row['main_net_inflow_volume']:15.0f} (特大单:{row['net_inflow_most_volume']:12.0f} 大单:{row['net_inflow_big_volume']:12.0f})")
    
    print(f"\n{'=' * 80}")
    print("【主力净流入(额) 排名 TOP10】")
    print('=' * 80)
    top_inflow_amount = analysis_df.nlargest(10, 'main_net_inflow_amount')[['stock_code', 'main_net_inflow_amount', 'net_inflow_most_amount', 'net_inflow_big_amount']]
    for idx, row in top_inflow_amount.iterrows():
        stock_name = C.get_stock_name(row['stock_code'])
        print(f"{row['stock_code']} {stock_name:8s} 主力净流入(额):{row['main_net_inflow_amount']:15.0f} (特大单:{row['net_inflow_most_amount']:12.0f} 大单:{row['net_inflow_big_amount']:12.0f})")
    
    print(f"\n{'=' * 80}")
    print("【大单买入占比排名 TOP10】")
    print('=' * 80)
    valid_df = analysis_df[analysis_df['bid_total_amount'] > 0]
    if not valid_df.empty:
        top_big_buy = valid_df.nlargest(10, 'big_buy_ratio')[['stock_code', 'big_buy_ratio']]
        for idx, row in top_big_buy.iterrows():
            stock_name = C.get_stock_name(row['stock_code'])
            ratio_pct = row['big_buy_ratio'] * 100
            print(f"{row['stock_code']} {stock_name:8s} 主力大单买入占比:{ratio_pct:6.2f}%")
    
    print(f"\n{'=' * 80}")
    print("【市场统计概览】")
    print('=' * 80)
    print(f"监控股票数: {len(analysis_df)}")
    print(f"DDX > 0 的股票数: {len(analysis_df[analysis_df['ddx'] > 0])}")
    print(f"主力净流入(量) > 0 的股票数: {len(analysis_df[analysis_df['main_net_inflow_volume'] > 0])}")
    print(f"主力净流入(额) > 0 的股票数: {len(analysis_df[analysis_df['main_net_inflow_amount'] > 0])}")
    print(f"平均DDX: {analysis_df['ddx'].mean():.4f}")
    print(f"平均主力净流入(量): {analysis_df['main_net_inflow_volume'].mean():.0f}")
    print(f"平均主力净流入(额): {analysis_df['main_net_inflow_amount'].mean():.0f}")
    
    print(f"\n{'=' * 80}")
    print(f"[{current_time}] L2大单统计处理完成")
    print('=' * 80)


def stop(C):
    """
    策略停止时执行
    """
    print("\n" + "=" * 60)
    print("L2大单统计策略已停止")
    print("=" * 60)
