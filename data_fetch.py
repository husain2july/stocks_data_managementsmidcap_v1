import os
import sqlite3
import logging
from datetime import datetime
import pytz
import yfinance as yf
import pandas as pd
from tabulate import tabulate


# ----------------------------
# CONFIGURATIONS
# ----------------------------
DB_NAME = "nifty50_top20.db"
README_FILE = "README.md"

# Top 20 NIFTY50 stocks (symbols must match Yahoo Finance format, ".NS" for NSE India)
STOCKS = [
    'ADANIGREEN.NS', 'ADANIPOWER.NS', 'ADANITRANS.NS', 'AFFLE.NS', 'AIAENG.NS',
    'AJANTPHARM.NS', 'APLLTD.NS', 'AMARAJABAT.NS', 'APOLLOTYRE.NS', 'ASHOKLEY.NS',
    'ASTRAL.NS', 'ATUL.NS', 'AUBANK.NS', 'BAJAJCON.NS', 'BAJAJHLDNG.NS',
    'BALRAMCHIN.NS', 'BATAINDIA.NS', 'BEL.NS', 'BHARATFORG.NS', 'BHEL.NS',
    'BSOFT.NS', 'CANFINHOME.NS', 'CHAMBLFERT.NS', 'CHOLAFIN.NS', 'CONCOR.NS',
    'COROMANDEL.NS', 'CROMPTON.NS', 'CUB.NS', 'DALBHARAT.NS', 'DEEPAKNTR.NS',
    'DELTACORP.NS', 'DIXON.NS', 'ESCORTS.NS', 'EXIDEIND.NS', 'FORTIS.NS',
    'GMRINFRA.NS', 'GNFC.NS', 'GODREJIND.NS', 'GRANULES.NS', 'GUJGASLTD.NS',
    'HAL.NS', 'HINDCOPPER.NS', 'HINDPETRO.NS', 'HONAUT.NS', 'ICICIGI.NS',
    'ICICIPRULI.NS', 'IEX.NS', 'IGL.NS', 'INDHOTEL.NS', 'INDIACEM.NS',
    'INDIAMART.NS', 'INDIANB.NS', 'INDIGO.NS', 'INDUSTOWER.NS', 'INTELLECT.NS',
    'IPCALAB.NS', 'IRB.NS', 'IRCTC.NS', 'JINDALSTEL.NS', 'JKCEMENT.NS',
    'JUSTDIAL.NS', 'KANSAINER.NS', 'KEI.NS', 'L&TFH.NS', 'LALPATHLAB.NS',
    'LAURUSLABS.NS', 'LICHSGFIN.NS', 'LTIM.NS', 'M&MFIN.NS', 'MANAPPURAM.NS',
    'MCX.NS', 'METROPOLIS.NS', 'MFSL.NS', 'MGL.NS', 'MOTHERSON.NS',
    'MUTHOOTFIN.NS', 'NATIONALUM.NS', 'NAUKRI.NS', 'NAVINFLUOR.NS', 'NMDC.NS',
    'OFSS.NS', 'OIL.NS', 'PEL.NS', 'PFC.NS', 'PIIND.NS',
    'POLYCAB.NS', 'RAIN.NS', 'RAJESHEXPO.NS', 'RAMCOCEM.NS', 'RBLBANK.NS',
    'RECLTD.NS', 'SAIL.NS', 'SBICARD.NS', 'SRF.NS', 'STARCEMENT.NS',
    'SUNTV.NS', 'SYNGENE.NS', 'TATACHEM.NS', 'TATACOMM.NS', 'TATAPOWER.NS',
    'TVSMOTOR.NS', 'UBL.NS', 'VOLTAS.NS', 'WHIRLPOOL.NS', 'ZOMATO.NS',
    'ZYDUSLIFE.NS', '3MINDIA.NS', 'AARTIIND.NS', 'ABBOTINDIA.NS', 'ABCAPITAL.NS',
    'ANGELONE.NS', 'APARINDS.NS', 'APCOTEXIND.NS', 'ASAHIINDIA.NS', 'ASHIANA.NS',
    'ASTRAZEN.NS', 'AVANTIFEED.NS', 'BALAMINES.NS', 'BALMLAWRIE.NS', 'BASF.NS',
    'BAYERCROP.NS', 'BEML.NS', 'BHARATGEAR.NS', 'BHARATRAS.NS', 'BIKAJI.NS',
    'BIRLACORPN.NS', 'BLISSGVS.NS', 'BLUEDART.NS', 'BLUESTARCO.NS', 'BOMDYEING.NS',
    'BORORENEW.NS', 'BRIGADE.NS', 'BSE.NS', 'CAMPUS.NS', 'CAMS.NS',
    'CAPLIPOINT.NS', 'CARBORUNIV.NS', 'CARE.NS', 'CARTRADE.NS', 'CASTROLIND.NS',
    'CCL.NS', 'CDSL.NS', 'CEATLTD.NS', 'CENTEXT.NS', 'CENTRALBK.NS',
    'CENTURYPLY.NS', 'CENTURYTEX.NS', 'CERA.NS', 'CHALET.NS', 'CHEMPLASTS.NS',
]

# Setup logging
logging.basicConfig(
    filename="data_fetch.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# IST timezone
IST = pytz.timezone("Asia/Kolkata")


# ----------------------------
# DATABASE FUNCTIONS
# ----------------------------
def init_db():
    """Ensure database exists with tables for each stock."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for stock in STOCKS:
        # table_name = stock.replace(".NS", ".NS")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS '{stock}' (
                datetime TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
        """)
    conn.commit()
    conn.close()


def insert_data(stock, df):
    """Insert stock data into database with ON CONFLICT IGNORE to avoid duplicates."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Convert datetime column to string
    df["datetime"] = df["datetime"].astype(str)
    df["volume"] = df["volume"].astype(int)
    print("My data is \n",df.head(2))

    rows = df[["datetime", "open", "high", "low", "close", "volume"]].values.tolist()
    print("rows", rows[:2])

    cursor.executemany(
        f"""
        INSERT OR IGNORE INTO '{stock}' (datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows
    )

    conn.commit()
    conn.close()
    logging.info(f"[{stock}] Inserted {len(rows)} rows (duplicates ignored)")

# ----------------------------
# DATA FETCHING
# ----------------------------
def fetch_stock_data(stock):
    """Fetch 1-min data for the past 15 minutes for a stock."""
    try:
        df = yf.download(
            tickers=stock,
            interval="1m",
            period="1d",
            progress=True)

        
        if df.empty:
            logging.warning(f"No data returned for {stock}")
            return None

        # Reset index to get datetime as column
        df = df.droplevel('Ticker', axis=1)
        df.reset_index(inplace=True)

        # Convert timezone to IST
        df["Datetime"] = df["Datetime"].dt.tz_convert(IST)
        
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)
        # df["Volume"] = df["Volume"] * 1 

        df.rename(columns={
            "Datetime": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        # df['volume'] = df['volume'].apply(lambda x: int.from_bytes(x, byteorder='little', signed=False))
        
        

        return df[["datetime", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logging.error(f"Error fetching data for {stock}: {e}")
        return None


# ----------------------------
# README UPDATE
# ----------------------------
def update_readme():
    """Append last 2 rows from each stock table to README.md using HTML table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write("# 📈 NIFTY50 Top 20 Data Snapshot\n\n")
        f.write(f"Last updated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")

        for stock in STOCKS:
            table_name = stock.replace(".NS", ".NS")
            try:
                df = pd.read_sql_query(
                    f"SELECT datetime, close, volume FROM '{table_name}' ORDER BY datetime DESC LIMIT 2", conn
                )
                if df.empty:
                    continue

                # Write HTML table
                f.write(f"## {stock}\n\n")
                f.write('<table>\n')
                f.write('  <tr><th>Datetime</th><th>Close</th><th>Volume</th></tr>\n')
                for _, row in df.iterrows():
                    f.write(f"  <tr><td>{row['datetime']}</td><td>{row['close']}</td><td>{row['volume']}</td></tr>\n")
                f.write('</table>\n\n')
            except Exception as e:
                logging.error(f"Error updating README for {stock}: {e}")

    conn.close()


# ----------------------------
# MAIN WORKFLOW
# ----------------------------
def main():
    logging.info("Starting data fetch cycle...")
    init_db()

    for stock in STOCKS:
        df = fetch_stock_data(stock)
        if df is not None and not df.empty:
            insert_data(stock, df)
            logging.info(f"Inserted {len(df)} rows for {stock}")
        else:
            logging.warning(f"No data to insert for {stock}")

    update_readme()
    logging.info("Cycle complete. README updated.")


if __name__ == "__main__":
    main()
