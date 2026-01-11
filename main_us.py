import yfinance as yf
import pandas as pd
import time
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests

# ===========================
# 1. 設定區
# ===========================

# Google Sheet 網址
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1mvC4i7Pw7uxS-OV5bav0uhvb6tAvRufTataFzwQQ2Ic/edit?usp=sharing'
SHEET_NAME = 'rsi_scanner_us'

# 濾網設定 (正式版)
MIN_PRICE = 5.0             # 股價 > 5 美元
MIN_VOLUME_SHARES = 200000  # 成交量 > 20 萬股

# 金鑰路徑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(BASE_DIR, 'service_account.json')

# ===========================
# 2. 技術指標計算
# ===========================
def calculate_sma(series, length):
    return series.rolling(window=length).mean()

def calculate_rsi(series, length=100):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/length, adjust=False).mean()
    ma_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = ma_up / ma_down
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ===========================
# 3. Google Sheet 存檔 (只存 日期/代號/名稱)
# ===========================
def update_rolling_data(new_data_list):
    print("\n正在連線 Google Sheet 更新資料...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL)
        
        try:
            ws = sheet.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            # 建立新工作表，只留 3 欄
            ws = sheet.add_worksheet(title=SHEET_NAME, rows="2000", cols="3")
            ws.append_row(["日期", "代號", "名稱"])

        all_rows = ws.get_all_values()
        if len(all_rows) <= 1:
            header = ["日期", "代號", "名稱"]
            existing_data = []
        else:
            header = all_rows[0]
            existing_data = all_rows[1:]

        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        today_rows = []
        
        for stock in new_data_list:
            # 簡化欄位，只存這三個
            row = [today_str, stock['ticker'], stock['name']]
            today_rows.append(row)

        clean_history = [row for row in existing_data if row[0] != today_str]
        final_data = clean_history + today_rows
        
        # 只保留最近 3 天
        unique_dates = sorted(list(set([row[0] for row in final_data])), reverse=True)
        if len(unique_dates) > 3:
            keep_dates = unique_dates[:3]
            final_data = [row for row in final_data if row[0] in keep_dates]
        
        ws.clear()
        ws.append_row(header)
        if final_data:
            ws.append_rows(final_data)
        print(f"✅ 更新完成！寫入 {len(today_rows)} 筆資料 (僅保留日期/代號/名稱)。")

    except Exception as e:
        print(f"❌ 存檔失敗: {e}")

# ===========================
# 4. 取得股票清單 (S&P 500 + 400)
# ===========================
def get_target_tickers():
    print("正在從 Wikipedia 抓取 S&P 500 與 S&P 400 清單...")
    tickers = []
    names = {}
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }

    try:
        # S&P 500
        url_500 = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        r_500 = requests.get(url_500, headers=headers)
        df500 = pd.read_html(r_500.text)[0]
        for _, row in df500.iterrows():
            sym = row['Symbol'].replace('.', '-')
            tickers.append(sym)
            names[sym] = row['Security']
            
        # S&P 400
        url_400 = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        r_400 = requests.get(url_400, headers=headers)
        df400 = pd.read_html(r_400.text)[0]
        
        col_sym = 'Symbol' if 'Symbol' in df400.columns else df400.columns[0]
        col_name = 'Security' if 'Security' in df400.columns else df400.columns[1]
        
        for _, row in df400.iterrows():
            sym = str(row[col_sym]).replace('.', '-')
            if sym not in tickers:
                tickers.append(sym)
                names[sym] = str(row[col_name])

        print(f"✅ 清單取得成功，共 {len(tickers)} 檔股票。")
        return tickers, names
    except Exception as e:
        print(f"❌ 無法取得清單: {e}")
        return [], {}

def check_stock(ticker, company_name):
    try:
        df = yf.download(ticker, period="2y", interval="1d", progress=False)
        
        if df.empty or len(df) < 300: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)

        today = df.iloc[-1]
        prev  = df.iloc[-2]
        
        if today['Close'] < MIN_PRICE: return None
        if today['Volume'] < MIN_VOLUME_SHARES: return None

        df['RSI'] = calculate_rsi(df['Close'], length=100)
        df['RSI_SMA'] = calculate_sma(df['RSI'], length=200)

        df['MA20']  = calculate_sma(df['Close'], length=20)
        df['MA60']  = calculate_sma(df['Close'], length=60)
        df['MA120'] = calculate_sma(df['Close'], length=120)
        df['MA240'] = calculate_sma(df['Close'], length=240)
        
        today = df.iloc[-1]
        prev  = df.iloc[-2]

        cond_rsi = (today['RSI'] > today['RSI_SMA'])
        above_all_now = (
            today['Close'] > today['MA20'] and 
            today['Close'] > today['MA60'] and 
            today['Close'] > today['MA120'] and 
            today['Close'] > today['MA240']
        )
        above_all_prev = (
            prev['Close'] > prev['MA20'] and 
            prev['Close'] > prev['MA60'] and 
            prev['Close'] > prev['MA120'] and 
            prev['Close'] > prev['MA240']
        )
        
        cond_first_day = above_all_now and (not above_all_prev)

        if cond_rsi and cond_first_day:
            return {
                "ticker": ticker, 
                "name": company_name
            }
        return None
    except Exception:
        return None

# ===========================
# 5. 主程式執行
# ===========================
if __name__ == "__main__":
    if not os.path.exists(JSON_FILE):
        print(f"❌ 錯誤: 找不到 {JSON_FILE}")
        exit()

    tickers, name_map = get_target_tickers()
    if not tickers: exit()

    found_stocks = []
    print(f"\n=== 開始掃描美股 (S&P 500+400) ===")
    
    start_time = time.time()

    for i, ticker in enumerate(tickers):
        if i % 10 == 0: print(".", end="", flush=True) # 簡易進度條
            
        c_name = name_map.get(ticker, ticker)
        res = check_stock(ticker, c_name)
        
        if res:
            print(f"\n🔥 發現: {res['ticker']} ({res['name']})")
            found_stocks.append(res)
        
        time.sleep(0.5)

    end_time = time.time()
    duration = (end_time - start_time) / 60
    
    print("\n" + "="*30)
    print(f"🎉 掃描完成！耗時 {duration:.1f} 分鐘。")
    print(f"共找到 {len(found_stocks)} 檔。")
    
    if found_stocks:
        update_rolling_data(found_stocks)
    else:
        print("今日無符合條件股票。")