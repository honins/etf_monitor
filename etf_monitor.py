import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
from fake_useragent import UserAgent
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple, Optional
import os
import sys
import matplotlib as mpl
import time
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from pathlib import Path

# 创建report目录
REPORT_DIR = Path("report")
REPORT_DIR.mkdir(exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(REPORT_DIR / 'etf_monitor.log')
    ]
)
logger = logging.getLogger(__name__)

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 设置请求重试策略
retry_strategy = Retry(
    total=5,  # 最大重试次数
    backoff_factor=1,  # 重试间隔
    status_forcelist=[500, 502, 503, 504],  # 需要重试的HTTP状态码
    allowed_methods=["GET", "POST"]  # 允许重试的HTTP方法
)

# 创建会话对象
session = requests.Session()
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=10,
    pool_maxsize=10
)
session.mount("http://", adapter)
session.mount("https://", adapter)

# 设置请求头
ua = UserAgent()
HEADERS = {
    'User-Agent': ua.random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache'
}

# 设置环境变量禁用代理
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

# 设置要监控的ETF代码
ETF_CODE = "512000"
# 报警阈值设置
ALERT_PRICE_CHANGE = 3.0  # 价格变动超过3%报警
ALERT_FLOW_MAIN = 10000   # 主力资金流入超过1亿元报警
CSV_FILE = REPORT_DIR / f"{ETF_CODE}_history.csv"  # 历史数据保存文件

# 配置参数
LOOKBACK_DAYS = 30   # 回看天数
MA_SHORT = 5         # 短期均线
MA_MEDIUM = 10       # 中期均线
MA_LONG = 20         # 长期均线
RSI_PERIOD = 14      # RSI周期
VOLUME_MA = 5        # 成交量均线周期

# 设置请求超时
REQUEST_TIMEOUT = 30  # 秒

def get_etf_data():
    """获取ETF实时数据"""
    try:
        url = f"http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&invt=2&fltt=2&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f57,f58,f60,f84,f85,f116&secid=1.{ETF_CODE}"
        logger.debug(f"请求URL: {url}")
        
        response = session.get(
            url, 
            headers=HEADERS, 
            verify=False, 
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.error(f"API请求失败，状态码: {response.status_code}")
            return None
            
        data = response.json()
        if 'data' not in data:
            logger.error(f"API返回数据格式错误: {data}")
            return None
            
        data = data['data']
        logger.debug(f"API返回数据: {data}")
        
        # 解析数据
        latest_price = data.get('f43', 0)
        prev_close = data.get('f60', 0)
        open_price = data.get('f46', 0)
        high_price = data.get('f44', 0)
        low_price = data.get('f45', 0)
        
        # 数据验证
        if not (0.1 <= latest_price <= 1000):
            logger.error(f"价格数据异常: {latest_price}")
            return None
            
        if not (0.1 <= prev_close <= 1000):
            logger.error(f"昨收价数据异常: {prev_close}")
            return None
            
        if not (0.1 <= open_price <= 1000):
            logger.error(f"开盘价数据异常: {open_price}")
            return None
            
        if not (0.1 <= high_price <= 1000):
            logger.error(f"最高价数据异常: {high_price}")
            return None
            
        if not (0.1 <= low_price <= 1000):
            logger.error(f"最低价数据异常: {low_price}")
            return None
        
        change = latest_price - prev_close
        change_pct = (change / prev_close) * 100 if prev_close != 0 else 0
        
        result = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "name": "券商ETF",
            "price": latest_price,
            "change": change,
            "change_pct": change_pct,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "prev_close": prev_close,
            "volume": data.get('f47', 0),
            "amount": data.get('f48', 0) / 10000,  # 转换为万元
            "turnover_rate": 0
        }
        
        logger.info(f"成功获取数据: {result}")
        return result
        
    except Exception as e:
        logger.error(f"获取数据失败: {str(e)}")
        return None

def get_historical_data():
    """获取历史数据"""
    try:
        url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg=0&end=20500101&secid=1.{ETF_CODE}&klt=101&fqt=1"
        logger.debug(f"请求URL: {url}")
        
        response = session.get(
            url, 
            headers=HEADERS, 
            verify=False, 
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.error(f"API请求失败，状态码: {response.status_code}")
            return pd.DataFrame()
            
        data = response.json()
        if 'data' not in data or 'klines' not in data['data']:
            logger.error(f"API返回数据格式错误: {data}")
            return pd.DataFrame()
            
        klines = data['data']['klines']
        records = []
        
        for kline in klines:
            fields = kline.split(',')
            records.append({
                '日期': fields[0],
                '开盘': float(fields[1]),
                '最高': float(fields[2]),
                '最低': float(fields[3]),
                '收盘': float(fields[4]),
                '成交量': float(fields[5])
            })
            
        hist_data = pd.DataFrame(records)
        hist_data['MA5'] = hist_data['收盘'].rolling(window=5).mean()
        hist_data['MA20'] = hist_data['收盘'].rolling(window=20).mean()
        
        logger.info(f"成功获取历史数据，共 {len(hist_data)} 条记录")
        return hist_data[['日期', '开盘', '最高', '最低', '收盘', '成交量', 'MA5', 'MA20']].tail(5)
        
    except Exception as e:
        logger.error(f"获取历史数据失败: {str(e)}")
        return pd.DataFrame()

def display_realtime_info(data: Dict):
    """显示实时交易信息"""
    if not data:
        logger.error("数据无效，无法显示")
        return
        
    # 数据验证
    if not (0.1 <= data['price'] <= 1000):
        logger.error(f"显示数据价格异常: {data['price']}")
        return
        
    clear_screen()
    print("="*80)
    print(f"ETF实时交易信息 - {data['name']}({ETF_CODE}) | 更新时间: {data['time']}")
    print("="*80)
    
    # 价格信息
    price_color = "\033[92m" if data['change'] >= 0 else "\033[91m"
    print(f"{price_color}最新价: {data['price']:.3f} | 涨跌: {data['change']:.3f} ({data['change_pct']:.2f}%)")
    
    # 显示基础价格数据
    print(f"今开: {data['open']:.3f} | 最高: {data['high']:.3f} | 最低: {data['low']:.3f} | 昨收: {data['prev_close']:.3f}")
    
    # 显示成交量和成交额（转换为亿元）
    volume = data['volume']
    amount = data['amount'] / 10000  # 转换为亿元
    print(f"成交量: {volume:,}手 | 成交额: {amount:.2f}亿元")
    
    print("\033[0m" + "="*80)
    print("按 Ctrl+C 停止监控...")

def clear_screen():
    """清屏函数，兼容不同操作系统"""
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    """主函数"""
    logger.info(f"开始监控ETF: {ETF_CODE}")
    
    try:
        while True:
            data = get_etf_data()
            if data:
                display_realtime_info(data)
            time.sleep(10)  # 每10秒更新一次
    except KeyboardInterrupt:
        logger.info("\n监控已停止")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")

if __name__ == "__main__":
    main()