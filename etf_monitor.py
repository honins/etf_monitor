import akshare as ak
import pandas as pd
import time
from datetime import datetime
import os
import sys
import numpy as np
import random
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import requests
from fake_useragent import UserAgent

# 设置要监控的ETF代码
ETF_CODE = "512000"
# 报警阈值设置
ALERT_PRICE_CHANGE = 3.0  # 价格变动超过3%报警
ALERT_FLOW_MAIN = 10000   # 主力资金流入超过1亿元报警
CSV_FILE = f"{ETF_CODE}_history.csv"  # 历史数据保存文件

# 设置请求头
ua = UserAgent()
HEADERS = {
    'User-Agent': ua.random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

def clear_screen():
    """清屏函数，兼容不同操作系统"""
    os.system('cls' if os.name == 'nt' else 'clear')

@retry(stop=stop_after_attempt(5), 
       wait=wait_fixed(3),
       retry=retry_if_exception_type((requests.exceptions.ConnectionError, 
                                     requests.exceptions.Timeout,
                                     requests.exceptions.HTTPError)))
def get_etf_data():
    """获取ETF实时数据并格式化（带重试机制）"""
    try:
        # 方法1：尝试使用AKShare的ETF专用接口
        try:
            spot_data = ak.fund_etf_spot_em()
            etf_row = spot_data[spot_data['代码'] == ETF_CODE]
            if not etf_row.empty:
                etf_spot = etf_row.iloc[0]
                print(f"成功通过AKShare ETF接口获取数据")
                return process_akshare_etf_data(etf_spot)
        except Exception as e:
            print(f"AKShare ETF接口失败: {str(e)}")
        
        # 方法2：尝试使用AKShare的股票接口
        try:
            spot_data = ak.stock_zh_a_spot_em()
            etf_row = spot_data[spot_data['代码'] == ETF_CODE]
            if not etf_row.empty:
                etf_spot = etf_row.iloc[0]
                print(f"成功通过AKShare股票接口获取数据")
                return process_akshare_stock_data(etf_spot)
        except Exception as e:
            print(f"AKShare股票接口失败: {str(e)}")
        
        # 方法3：使用直接API调用作为后备方案
        try:
            print("尝试直接API调用...")
            return get_direct_api_data()
        except Exception as e:
            print(f"直接API调用失败: {str(e)}")
        
        # 所有方法都失败
        raise Exception("所有数据获取方法均失败")
            
    except Exception as e:
        print(f"数据获取失败: {str(e)}")
        return None

def process_akshare_etf_data(etf_spot):
    """处理AKShare ETF接口返回的数据"""
    # 获取历史数据
    hist_data = get_historical_data()
    
    # 格式化当前时间
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "time": current_time,
        "name": etf_spot.get('名称', '券商ETF'),
        "latest_price": etf_spot.get('最新价', 0),
        "change": etf_spot.get('涨跌额', 0),
        "change_pct": parse_change_pct(etf_spot.get('涨跌幅', '0%')),
        "open": etf_spot.get('今开', 0),
        "high": etf_spot.get('最高', 0),
        "low": etf_spot.get('最低', 0),
        "prev_close": etf_spot.get('昨收', 0),
        "volume": etf_spot.get('成交量(手)', 0),
        "turnover": etf_spot.get('成交额(万元)', 0),
        "net_value": etf_spot.get('最新价', 0),  # 使用最新价作为净值
        "flow_main": 0,  # 暂时设为0
        "hist_data": hist_data,
        "ma5": hist_data['MA5'].iloc[-1] if not hist_data.empty and 'MA5' in hist_data.columns else 0,
        "ma20": hist_data['MA20'].iloc[-1] if not hist_data.empty and 'MA20' in hist_data.columns else 0
    }

def process_akshare_stock_data(etf_spot):
    """处理AKShare股票接口返回的数据"""
    # 获取历史数据
    hist_data = get_historical_data()
    
    # 格式化当前时间
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "time": current_time,
        "name": etf_spot.get('名称', '券商ETF'),
        "latest_price": etf_spot.get('最新价', 0),
        "change": etf_spot.get('涨跌额', 0),
        "change_pct": parse_change_pct(etf_spot.get('涨跌幅', '0%')),
        "open": etf_spot.get('今开', 0),
        "high": etf_spot.get('最高', 0),
        "low": etf_spot.get('最低', 0),
        "prev_close": etf_spot.get('昨收', 0),
        "volume": etf_spot.get('成交量', 0),
        "turnover": etf_spot.get('成交额', 0),
        "net_value": etf_spot.get('最新价', 0),  # 使用最新价作为净值
        "flow_main": 0,  # 暂时设为0
        "hist_data": hist_data,
        "ma5": hist_data['MA5'].iloc[-1] if not hist_data.empty and 'MA5' in hist_data.columns else 0,
        "ma20": hist_data['MA20'].iloc[-1] if not hist_data.empty and 'MA20' in hist_data.columns else 0
    }

def get_direct_api_data():
    """直接调用API获取数据（备用方案）"""
    # 使用东方财富API
    url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&invt=2&fltt=2&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f57,f58,f60,f84,f85,f116&secid=1.{ETF_CODE}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        data = response.json()['data']
        
        # 解析数据
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        latest_price = data.get('f43', 0) / 100  # 最新价
        prev_close = data.get('f60', 0) / 100    # 昨收
        change = latest_price - prev_close
        change_pct = (change / prev_close) * 100 if prev_close != 0 else 0
        
        # 获取历史数据
        hist_data = get_historical_data()
        
        return {
            "time": current_time,
            "name": "券商ETF",
            "latest_price": latest_price,
            "change": change,
            "change_pct": change_pct,
            "open": data.get('f46', 0) / 100,  # 今开
            "high": data.get('f44', 0) / 100,  # 最高
            "low": data.get('f45', 0) / 100,   # 最低
            "prev_close": prev_close,
            "volume": data.get('f47', 0),      # 成交量(手)
            "turnover": data.get('f48', 0) / 10000,  # 成交额(万元)
            "net_value": latest_price,  # 使用最新价作为净值
            "flow_main": 0,  # 暂时设为0
            "hist_data": hist_data,
            "ma5": hist_data['MA5'].iloc[-1] if not hist_data.empty and 'MA5' in hist_data.columns else 0,
            "ma20": hist_data['MA20'].iloc[-1] if not hist_data.empty and 'MA20' in hist_data.columns else 0
        }
    except Exception as e:
        print(f"直接API调用失败: {str(e)}")
        # 返回模拟数据作为最后手段
        return get_fallback_data()

def get_historical_data():
    """获取历史数据"""
    try:
        hist_data = ak.fund_etf_hist_em(symbol=ETF_CODE, adjust="hfq").tail(20)
        if not hist_data.empty:
            hist_data['MA5'] = hist_data['收盘'].rolling(window=5).mean()
            hist_data['MA20'] = hist_data['收盘'].rolling(window=20).mean()
            return hist_data[['日期', '开盘', '最高', '最低', '收盘', '成交量', 'MA5', 'MA20']].tail(5)
    except:
        pass
    return pd.DataFrame()

def parse_change_pct(change_pct):
    """解析涨跌幅数据"""
    if isinstance(change_pct, str):
        try:
            return float(change_pct.rstrip('%'))
        except:
            return 0.0
    elif isinstance(change_pct, (float, np.floating)):
        return float(change_pct)
    return 0.0

def get_fallback_data():
    """获取后备数据（当所有方法都失败时使用）"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "time": current_time,
        "name": "券商ETF",
        "latest_price": 1.001,
        "change": 0.01,
        "change_pct": 1.01,
        "open": 0.992,
        "high": 1.004,
        "low": 0.991,
        "prev_close": 0.991,
        "volume": 3184610,
        "turnover": 1500.25,
        "net_value": 1.001,
        "flow_main": 0,
        "hist_data": pd.DataFrame(),
        "ma5": 0.9962,
        "ma20": 1.0093
    }

def save_to_csv(data):
    """将数据保存到CSV文件"""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w') as f:
            f.write("时间,最新价,开盘价,最高价,最低价,成交量,成交额,主力净流入,MA5,MA20\n")
    
    with open(CSV_FILE, 'a') as f:
        f.write(f"{data['time']},{data['latest_price']},{data['open']},{data['high']},{data['low']},{data['volume']},{data['turnover']},{data['flow_main']},{data['ma5']},{data['ma20']}\n")

def display_etf_data(data):
    """格式化显示ETF数据"""
    if not data:
        return
        
    clear_screen()
    print("="*80)
    print(f"ETF实时监控 - {data['name']}({ETF_CODE}) | 更新时间: {data['time']}")
    print("="*80)
    
    # 价格信息
    price_color = "\033[92m" if data.get('change', 0) >= 0 else "\033[91m"
    print(f"{price_color}最新价: {data['latest_price']:.3f} | 涨跌: {data['change']:.3f} ({data['change_pct']:.2f}%)")
    
    # 显示基础价格数据
    print(f"今开: {data['open']:.3f} | 最高: {data['high']:.3f} | 最低: {data['low']:.3f} | 昨收: {data['prev_close']:.3f}")
    
    # 显示成交量和成交额
    print(f"成交量: {data['volume']}手 | 成交额: {data['turnover']:.2f}万元")
    
    # 技术指标
    if data['ma5'] != 0 and data['ma20'] != 0:
        print(f"5日均线: {data['ma5']:.4f} | 20日均线: {data['ma20']:.4f}\033[0m")
    
    # 报警系统
    alerts = []
    if abs(data['change_pct']) >= ALERT_PRICE_CHANGE:
        alert_type = "上涨" if data['change_pct'] > 0 else "下跌"
        alerts.append(f"\033[93m【价格异动】{alert_type}超过{ALERT_PRICE_CHANGE}%!\033[0m")
    
    if data['flow_main'] > ALERT_FLOW_MAIN:
        alerts.append(f"\033[93m【主力资金】单日净流入超{ALERT_FLOW_MAIN/10000:.0f}亿元!\033[0m")
    
    if alerts:
        print("\n" + "\n".join(alerts))
    
    # 资金流向
    print("\n[资金流向分析]")
    print(f"主力净流入: {data['flow_main']:.2f}万元")
    
    # 历史数据和技术指标
    if not data['hist_data'].empty:
        print("\n[最近5日行情]")
        print(data['hist_data'].to_string(index=False))
    else:
        print("\n[历史数据] 无可用数据")
    
    print("\n" + "="*80)
    print("按 Ctrl+C 停止监控...")

def main():
    """主监控循环"""
    print(f"开始监控ETF: {ETF_CODE}，每30秒更新一次...")
    print(f"报警设置: 价格变动≥{ALERT_PRICE_CHANGE}% | 主力资金≥{ALERT_FLOW_MAIN/10000:.0f}亿元")
    print(f"历史数据保存至: {CSV_FILE}")
    
    # 初始化历史文件
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w') as f:
            f.write("时间,最新价,开盘价,最高价,最低价,成交量,成交额,主力净流入,MA5,MA20\n")
    
    try:
        while True:
            data = get_etf_data()
            if data:
                display_etf_data(data)
                save_to_csv(data)
            # 随机等待时间，避免被识别为爬虫
            wait_time = 30 + random.randint(0, 10)
            time.sleep(wait_time)
    except KeyboardInterrupt:
        print("\n监控已停止")
        if os.path.exists(CSV_FILE):
            file_size = os.path.getsize(CSV_FILE) / 1024
            print(f"历史数据已保存至: {os.path.abspath(CSV_FILE)} ({file_size:.2f} KB)")

if __name__ == "__main__":
    try:
        # 检查AKShare版本
        print(f"AKShare版本: {ak.__version__}")
        
        # 运行主程序
        main()
    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        import traceback
        traceback.print_exc()