import csv
import time
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
import os
import math
from typing import List, Dict, Any, Tuple, Optional
import sys

# =================== CONFIGURABLE PARAMETERS ===================
BASE_URL = "https://api.binance.com"
LOG_FOLDER = "binance_logs"

ALERT_CONFIG = {
    "enable_logging": True,  # Master switch for logging to file
    "alerts": {
        "bullish_long": {
            "enabled": True,
            "min_bullish_percentage": 3.0,
            "min_turnover": 100.0
        },
        "bullish_marubozo": {
            "enabled": True,
            "min_bullish_percentage": 1.0,
            "min_turnover": 100.0,
            "debug": False
        },
        "bullish_flag": {
            "enabled": True,
            "min_bullish_consecutive": 3,
            "max_bullish_consecutive": 5,
            "min_bearish_consecutive": 2,
            "max_bearish_consecutive": 3,
            "min_candle_change_percent": 0.1,
            "max_bearish_drop_percent": 50.0,
            "min_turnover": 500.0,
            "debug": False
        },
        "bullish_series": {
            "enabled": True,
            "min_consecutive_bullish": 3,
            "max_consecutive_bullish": 15,
            "min_candle_change_percent": 0.2,
            "alert_on_each_candle": True,
            "require_increasing_closes": False,
            "min_total_series_gain": 0.5,
            "min_turnover": 500.0,
            "debug": False
        }
        "bullish_consecutive": {  # BC Alert
            "enabled": True,
            "min_consecutive": 3,  # Minimum candles in series
            "max_consecutive": 15,  # Maximum candles before reset
            "min_total_gain_percent": 0.0,  # Optional: Min total gain % (0.0 = any non-bearish series)
            "include_doji": True,   # Explicitly flag dojis as allowed
            "require_increasing_closes": False,  # Optional: enforce higher closes
            "min_turnover": 50.0,  # Optional: filter low-volume patterns
            "debug": False
        }
    }
}

SESSION_CONFIG = {
    "timeout": 10,
    "connector_limit": 100,
    "ttl_dns_cache": 300
}

CANDLE_HISTORY_CONFIG = {
    "initial_limit": 35,
    "update_limit": 2,
    "max_retries": 3,
    "required_history": 30
}

MOVING_AVERAGES_CONFIG = {
    "window": 10
}

TIMEZONE_CONFIG = {
    "vienna_offset": timedelta(hours=2)  # UTC+2 for Vienna
}

CSV_CONFIG = {
    "save_binance_scanner_csv": False
}
# =================== END OF CONFIGURABLE PARAMETERS ===================

session = None
pair_candles = {}
log_file = None
script_start_time = None

def setup_logging(enable_logging: bool):
    global log_file, script_start_time
    script_start_time = datetime.now(timezone.utc).astimezone(timezone(TIMEZONE_CONFIG["vienna_offset"]))
    if enable_logging:
        if not os.path.exists(LOG_FOLDER):
            os.makedirs(LOG_FOLDER)
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(LOG_FOLDER, f"{current_time}_binance_scanner_logs.txt")
        log_file = open(log_file_path, "w")
    else:
        log_file = None

def log_message(message: str, is_alert=False):
    timestamp = datetime.now(timezone.utc).astimezone(timezone(TIMEZONE_CONFIG["vienna_offset"])).strftime("%H:%M:%S")

    if ALERT_CONFIG["enable_logging"] and log_file:
        log_file.write(f"{timestamp} {message}\n")
        log_file.flush()

    if is_alert or message.startswith(("Starting Binance", "PHASE", "PREPARING", "Scanner stopped")):
        if is_alert:
            print(message)
        else:
            print(f"{timestamp} {message}")

def print_with_timestamp(message: str):
    log_message(message)

def get_vienna_time(utc_time=None):
    if utc_time is None:
        utc_time = datetime.now(timezone.utc)
    return utc_time.astimezone(timezone(TIMEZONE_CONFIG["vienna_offset"]))

async def get_session():
    global session
    if session is None:
        timeout = aiohttp.ClientTimeout(total=SESSION_CONFIG["timeout"])
        conn = aiohttp.TCPConnector(limit=SESSION_CONFIG["connector_limit"],
                                    ttl_dns_cache=SESSION_CONFIG["ttl_dns_cache"])
        session = aiohttp.ClientSession(connector=conn, timeout=timeout)
    return session

async def fetch_usdt_pairs_with_usdc_counterparts() -> List[str]:
    log_message("Fetching list of actively tradeable USDT pairs with USDC counterparts from Binance...")
    session = await get_session()
    try:
        async with session.get(f"{BASE_URL}/api/v3/exchangeInfo") as response:
            if response.status == 200:
                data = await response.json()
                all_pairs = [symbol["symbol"] for symbol in data["symbols"] if symbol["status"] == "TRADING"]

                # Get USDT pairs
                usdt_pairs = [symbol for symbol in all_pairs if symbol.endswith("USDT")]

                # Get USDC pairs
                usdc_pairs = set([symbol for symbol in all_pairs if symbol.endswith("USDC")])

                # Filter USDT pairs to only those with USDC counterparts
                filtered_pairs = []
                for pair in usdt_pairs:
                    base_currency = pair[:-4]  # Remove USDT suffix
                    usdc_pair = f"{base_currency}USDC"
                    if usdc_pair in usdc_pairs:
                        filtered_pairs.append(pair)

                log_message(f"Successfully retrieved {len(filtered_pairs)} USDT trading pairs with USDC counterparts")
                return filtered_pairs
            else:
                error_text = await response.text()
                log_message(f"Failed to fetch pairs. Status Code: {response.status}, Message: {error_text}")
                return []
    except Exception as e:
        log_message(f"Error fetching pairs: {e}")
        return []

async def fetch_ohlc_for_pair(pair: str, params: Dict[str, Any]) -> Tuple[str, Optional[List[Any]], Optional[str]]:
    session = await get_session()
    retry_count = 0
    max_retries = CANDLE_HISTORY_CONFIG["max_retries"]

    binance_params = {
        'symbol': pair,
        'interval': '1m',
        'limit': params.get('limit', 200)
    }

    while retry_count < max_retries:
        try:
            async with session.get(f"{BASE_URL}/api/v3/klines", params=binance_params) as response:
                if response.status == 200:
                    data = await response.json()
                    used_weight = response.headers.get("X-MBX-USED-WEIGHT-1m", "N/A")

                    # Binance returns: [Open time, Open, High, Low, Close, Volume, Close time, ...]
                    converted_data = [
                        [
                            int(candle[0]),       # Open time
                            float(candle[1]),     # Open
                            float(candle[2]),     # High
                            float(candle[3]),     # Low
                            float(candle[4]),     # Close
                            float(candle[5]),     # Volume
                            float(candle[7])      # Quote asset volume (turnover)
                        ] 
                        for candle in data
                    ]
                    return pair, converted_data, used_weight
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    log_message(f"Rate limit exceeded for {pair}. Waiting for {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                else:
                    error_text = await response.text()
                    log_message(f"Failed to fetch data for {pair}. Status Code: {response.status}, Message: {error_text}")
                    return pair, None, None
        except Exception as e:
            log_message(f"Error fetching data for {pair} (attempt {retry_count + 1}/{max_retries}): {e}")
            retry_count += 1
            await asyncio.sleep(1)

    return pair, None, None

async def backfill_gaps(pair: str, gaps: List[Tuple[int, int]]) -> bool:
    if not gaps:
        return True

    session = await get_session()
    max_gap_minutes = 35
    max_retries = 3

    for gap_start, gap_end in gaps:
        gap_duration_minutes = (gap_end - gap_start) // 60000 + 1

        if gap_duration_minutes > max_gap_minutes:
            log_message(f"Gap too large to backfill for {pair}: {gap_duration_minutes} minutes (max {max_gap_minutes})")
            continue

        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                params = {
                    'symbol': pair,
                    'interval': '1m',
                    'startTime': gap_start,
                    'endTime': gap_end,
                    'limit': gap_duration_minutes
                }

                async with session.get(f"{BASE_URL}/api/v3/klines", params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        backfilled_candles = [
                            [
                                int(candle[0]),
                                float(candle[1]),
                                float(candle[2]),
                                float(candle[3]),
                                float(candle[4]),
                                float(candle[5]),
                                float(candle[7]),  # Quote asset volume as turnover
                            ] 
                            for candle in data
                        ]

                        if backfilled_candles:
                            existing_candles = pair_candles.get(pair, [])
                            merged = existing_candles + backfilled_candles
                            merged = sorted(merged, key=lambda x: x[0])
                            pair_candles[pair] = merged
                            log_message(f"Successfully backfilled {len(backfilled_candles)} candles for {pair}")
                            success = True
                        else:
                            log_message(f"No candles returned for backfill of {pair}")
                            retry_count += 1
                            await asyncio.sleep(1)
                    else:
                        error_text = await response.text()
                        log_message(f"Failed to backfill data for {pair}. Status Code: {response.status}, Message: {error_text}")
                        retry_count += 1
                        await asyncio.sleep(1)
            except Exception as e:
                log_message(f"Error during backfill for {pair}: {e}")
                retry_count += 1
                await asyncio.sleep(1)

    current_gaps = detect_gaps(pair_candles.get(pair, []))
    return len(current_gaps) == 0

def are_candles_consecutive(candles: List[List[Any]], interval_ms: int = 60000) -> bool:
    sorted_candles = sorted(candles, key=lambda x: x[0])
    for i in range(1, len(sorted_candles)):
        current_time = sorted_candles[i][0]
        previous_time = sorted_candles[i-1][0]
        if current_time - previous_time != interval_ms:
            return False
    return True

def calculate_moving_averages(candles: List[List[Any]], window: int = MOVING_AVERAGES_CONFIG["window"]) -> Tuple[float, float, float]:
    if len(candles) < window:
        return None, None, None

    recent_candles = candles[-window:]

    try:
        volumes = [float(candle[5]) for candle in recent_candles]
        volume_ma = sum(volumes) / window

        price_lengths = [abs(float(candle[4]) - float(candle[1])) for candle in recent_candles]
        price_length_ma = sum(price_lengths) / window

        total_ranges = [abs(float(candle[2]) - float(candle[3])) for candle in recent_candles]
        total_range_ma = sum(total_ranges) / window

        return volume_ma, price_length_ma, total_range_ma
    except Exception as e:
        log_message(f"Error calculating moving averages: {str(e)}")
        return None, None, None

def detect_gaps(candles: List[List[Any]], interval_ms: int = 60000) -> List[Tuple[int, int]]:
    if not candles or len(candles) < 2:
        return []

    sorted_candles = sorted(candles, key=lambda x: x[0])
    gaps = []

    for i in range(1, len(sorted_candles)):
        current_time = sorted_candles[i][0]
        previous_time = sorted_candles[i-1][0]
        expected_time = previous_time + interval_ms

        if current_time > expected_time:
            gaps.append((expected_time, current_time - interval_ms))

    return gaps

def save_candles_to_csv(pair_candles: Dict[str, List[List[Any]]], prefix: str = "initial"):
    if not CSV_CONFIG["save_binance_scanner_csv"]:
        return

    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)

    vienna_time = get_vienna_time()
    filename = f"{LOG_FOLDER}/{vienna_time.strftime('%Y%m%d_%H%M')}_Binance_Scanner_{prefix}.csv"

    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([
                'pair', 'timestamp', 'open_time_utc', 'open_time_vienna', 
                'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])

            for pair, candles in pair_candles.items():
                for candle in candles:
                    utc_time = datetime.fromtimestamp(candle[0]/1000, tz=timezone.utc)
                    vienna_time = get_vienna_time(utc_time)
                    writer.writerow([
                        pair,
                        candle[0],
                        utc_time.strftime('%Y-%m-%d %H:%M:%S'),
                        vienna_time.strftime('%Y-%m-%d %H:%M:%S'),
                        candle[1],  # open
                        candle[2],  # high
                        candle[3],  # low
                        candle[4],  # close
                        candle[5],  # volume
                        candle[6]   # turnover
                    ])

        if CSV_CONFIG["save_binance_scanner_csv"]:
            log_message(f"Candle data saved to: {filename}")
    except Exception as e:
        log_message(f"Error saving CSV file: {e}")

async def initial_fetch(usdt_pairs):
    log_message("\n=== PHASE 1: INITIAL DATA FETCH ===")
    log_message(f"Fetching {CANDLE_HISTORY_CONFIG['initial_limit']} candles each for {len(usdt_pairs)} pairs")
    log_message("This may take several seconds...")

    params = {
        "interval": "1m",
        "limit": CANDLE_HISTORY_CONFIG["initial_limit"]
    }
    global pair_candles
    pair_candles = {}
    start_time = time.time()

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    log_message(f"Current reference time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    batch_size = 100
    batches = [usdt_pairs[i:i + batch_size] for i in range(0, len(usdt_pairs), batch_size)]
    total_pairs_processed = 0

    for batch_index, batch in enumerate(batches):
        log_message(f"Processing batch {batch_index + 1}/{len(batches)} ({len(batch)} pairs)")
        tasks = [fetch_ohlc_for_pair(pair, params) for pair in batch]
        results = await asyncio.gather(*tasks)

        batch_pairs_processed = 0
        for pair, ohlc_data, used_weight in results:
            if ohlc_data:
                valid_candles = []
                for entry in ohlc_data:
                    utc_open_time = datetime.fromtimestamp(entry[0] / 1000, tz=timezone.utc)
                    if utc_open_time >= now:
                        continue
                    valid_candles.append(entry)

                if valid_candles:
                    pair_candles[pair] = valid_candles

                    gaps = detect_gaps(valid_candles)
                    if gaps:
                        log_message(f"Found {len(gaps)} gaps in initial data for {pair}")
                        await backfill_gaps(pair, gaps)

                    batch_pairs_processed += 1
                await asyncio.sleep(0.02)

        total_pairs_processed += batch_pairs_processed
        log_message(f"Batch {batch_index + 1} complete - {batch_pairs_processed}/{len(batch)} pairs processed")

    save_candles_to_csv(pair_candles, "initial")

    if usdt_pairs:
        sample_pair = usdt_pairs[0]
        if sample_pair in pair_candles:
            candles = pair_candles[sample_pair]
            log_message(f"\nSample data for {sample_pair}:")
            log_message(f"Total candles: {len(candles)}")

    end_time = time.time()
    log_message(f"\n=== INITIAL FETCH COMPLETE ===")
    log_message(f"Fetched data for {total_pairs_processed}/{len(usdt_pairs)} pairs")
    log_message(f"Time taken: {end_time - start_time:.2f} seconds")

    return pair_candles

async def update_pair_data(pair: str, current_minute: datetime) -> Tuple[bool, Dict[str, Any]]:
    params = {
        "interval": "1m",
        "limit": CANDLE_HISTORY_CONFIG["update_limit"]
    }
    max_retries = CANDLE_HISTORY_CONFIG["max_retries"]
    retry_count = 0
    success = False
    stats = {}

    while retry_count < max_retries and not success:
        pair_result, ohlc_data, used_weight = await fetch_ohlc_for_pair(pair, params)
        if ohlc_data and len(ohlc_data) > 0:
            target_candles = []
            for entry in ohlc_data:
                candle_time = datetime.fromtimestamp(entry[0] / 1000, tz=timezone.utc)
                candle_minute = candle_time.replace(second=0, microsecond=0)
                if candle_minute == current_minute - timedelta(minutes=1):
                    target_candles.append(entry)

            if target_candles:
                new_candle = target_candles[0]
                if pair in pair_candles:
                    last_candle_time = pair_candles[pair][-1][0] if pair_candles[pair] else 0
                    expected_next_time = last_candle_time + 60000

                    if new_candle[0] > expected_next_time:
                        gap_start = expected_next_time
                        gap_end = new_candle[0] - 60000
                        gaps = [(gap_start, gap_end)]
                        log_message(f"Detected gap for {pair} before new candle - attempting backfill")
                        await backfill_gaps(pair, gaps)

                    if len(pair_candles[pair]) >= CANDLE_HISTORY_CONFIG["required_history"]:
                        pair_candles[pair].pop(0)
                    pair_candles[pair].append(new_candle)

                    if len(pair_candles[pair]) >= MOVING_AVERAGES_CONFIG["window"]:
                        volume_ma, price_length_ma, total_range_ma = calculate_moving_averages(pair_candles[pair])
                        stats = {
                            "volume_ma": volume_ma,
                            "price_length_ma": price_length_ma,
                            "total_range_ma": total_range_ma,
                            "weight": used_weight,
                            "candle_time": candle_minute.strftime("%H:%M:%S")
                        }
                        success = True
                    else:
                        log_message(f"Not enough candles for {pair} to calculate MA ({len(pair_candles[pair])}/{MOVING_AVERAGES_CONFIG['window']})")
                else:
                    log_message(f"No candle history for {pair}")
            else:
                log_message(f"No candle found for {pair} at {(current_minute - timedelta(minutes=1)).strftime('%H:%M:%S')}")
                retry_count += 1
                await asyncio.sleep(1)
        else:
            log_message(f"Failed to fetch data for {pair} (attempt {retry_count + 1}/{max_retries})")
            retry_count += 1
            await asyncio.sleep(1)

    return success, stats

# ==================== BULLISH LONG CANDLE (BL) ALERT BEGINN ==================================

def check_for_long_bullish_candle(
        pair: str,
        candle: List[Any],
        min_bullish_percentage: float = ALERT_CONFIG["alerts"]["bullish_long"]["min_bullish_percentage"]
) -> Optional[Dict[str, Any]]:
    if not ALERT_CONFIG["alerts"]["bullish_long"]["enabled"]:
        return None

    open_price = float(candle[1])
    close_price = float(candle[4])
    turnover = float(candle[6])

    # Check minimum turnover first
    if turnover < ALERT_CONFIG["alerts"]["bullish_long"]["min_turnover"]:
        return None

    # First check if it's bullish and meets minimum percentage requirement
    if close_price <= open_price:
        return None

    price_change_percent = ((close_price - open_price) / open_price) * 100
    if price_change_percent < min_bullish_percentage:
        return None

    candle_time = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
    vienna_time = get_vienna_time(candle_time)

    return {
        "pair": pair,
        # "candle_time_vienna": vienna_time.strftime("%H:%M:%S"),
        "candle_time_vienna": vienna_time.strftime("%H:%M"),  # Changed from "%H:%M:%S"
        "price_change_percent": price_change_percent,
        "turnover": float(candle[6]),
        "status": "valid"
    }

# ==================== BULLISH LONG CANDLE (BL) ALERT END ==================================

# ==================== BULLISH MARUBOZO (BM) ALERT BEGINN ==================================

def check_for_marubozo_bullish_candle(
        pair: str,
        candle: List[Any],
        min_bullish_percentage: float = ALERT_CONFIG["alerts"]["bullish_marubozo"]["min_bullish_percentage"]
) -> Optional[Dict[str, Any]]:
    if not ALERT_CONFIG["alerts"]["bullish_marubozo"]["enabled"]:
        return None

    # Extract prices with float conversion
    open_price, high_price, low_price, close_price, volume, turnover = map(float, candle[1:7])

    # Check minimum turnover first
    if turnover < ALERT_CONFIG["alerts"]["bullish_marubozo"]["min_turnover"]:
        return None

    # 1. Absolute wickless check (no tolerance)
    if not (math.isclose(open_price, low_price, rel_tol=0) and 
            math.isclose(close_price, high_price, rel_tol=0)):
        return None

    # 2. Bullish and meets minimum percentage
    price_change = ((close_price - open_price) / open_price) * 100
    if price_change < min_bullish_percentage:
        return None

    # Valid Marubozo
    candle_time = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
    return {
        "pair": pair,
        "alert_type": "BM",
        # "candle_time_vienna": get_vienna_time(candle_time).strftime("%H:%M:%S"),
        "candle_time_vienna": get_vienna_time(candle_time).strftime("%H:%M"),  # Changed to HH:MM format
        "price_change_percent": round(price_change, 2),
        "turnover": float(candle[6]),
        "status": "valid"  # Standard status like other alerts
    }

# ==================== BULLISH MARUBOZO (BM) ALERT END =====================================

# ==================== BULLISH FLAG (BF) ALERT BEGINN ======================================
def check_for_bf_alert(pair: str, candles: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """
    Check for Bullish Flag (BF) alert pattern in candle data.
    Pattern requires:
    1. 3-5 consecutive bullish candles followed by 
    2. 2-3 consecutive bearish candles
    3. More bullish than bearish candles
    4. Bearish drop <= max allowed percentage of bullish gain
    
    Args:
        pair: Trading pair symbol (e.g., 'BTCUSDT')
        candles: List of candle data where each candle is:
            [timestamp, open, high, low, close, volume, turnover]
    
    Returns:
        Dict with alert details if pattern found, None otherwise
        Format:
        {
            "pair": str,
            "alert_type": "BF",
            "candle_time_vienna": str,
            "bullish_count": int,
            "bearish_count": int,
            "bearish_drop_percent": float,
            "turnover": float,
            "status": "valid"
        }
    """
    # Skip if BF alerts are disabled in config
    if not ALERT_CONFIG["alerts"]["bullish_flag"]["enabled"]:
        return None

    # Validate input
    if not candles or len(candles) < 5:
        return None

    # Get and validate configuration parameters
    config = ALERT_CONFIG["alerts"]["bullish_flag"]
    min_change = config["min_candle_change_percent"]

    # Validate configuration
    if config["min_bullish_consecutive"] > config["max_bullish_consecutive"]:
        log_message(f"Invalid BF config: min_bullish_consecutive > max_bullish_consecutive")
        return None

    if config["min_bearish_consecutive"] > config["max_bearish_consecutive"]:
        log_message(f"Invalid BF config: min_bearish_consecutive > max_bearish_consecutive")
        return None

    try:
        # --- STEP 1: Verify latest candle is bearish ---
        latest_candle = candles[-1]
        if len(latest_candle) < 5:
            log_message(f"Invalid candle format for {pair}")
            return None

        latest_open = float(latest_candle[1])
        latest_close = float(latest_candle[4])

        # Avoid division by zero
        if latest_open == 0:
            log_message(f"Zero open price for {pair}")
            return None

        latest_change = abs((latest_close - latest_open) / latest_open) * 100

        # Latest candle must be bearish with sufficient change
        if latest_change < min_change or latest_close >= latest_open:
            return None

        # --- STEP 2: Count consecutive bearish candles from end ---
        bearish_count = 0
        first_bearish_open = None  # Open price of FIRST bearish candle in sequence
        last_bearish_close = None  # Close price of LAST bearish candle in sequence
        turnover = 0.0

        for i in range(len(candles) - 1, -1, -1):  # Iterate backwards
            candle = candles[i]
            if len(candle) < 5:
                break

            open_price = float(candle[1])
            close_price = float(candle[4])

            if open_price == 0:
                break

            change_percent = abs((close_price - open_price) / open_price) * 100

            # Check if candle is bearish with sufficient change
            if change_percent >= min_change and close_price < open_price:
                bearish_count += 1
                turnover += float(candle[6])
                if last_bearish_close is None:    # Only set for latest candle (first iteration)
                    last_bearish_close = close_price
                first_bearish_open = open_price   # This will end up being the FIRST bearish candle's open
            else:
                break  # Stop at first non-bearish candle

        # Verify bearish count is within configured range
        if not (config["min_bearish_consecutive"] <= bearish_count <= config["max_bearish_consecutive"]):
            return None

        # --- STEP 3: Count bullish candles before bearish sequence ---
        bullish_count = 0
        bullish_start_price = None  # Open price of FIRST bullish candle
        bullish_end_price = None   # Close price of LAST bullish candle
        start_idx = len(candles) - bearish_count - 1  # Start before bearish sequence

        for i in range(start_idx, -1, -1):  # Iterate backwards
            candle = candles[i]
            if len(candle) < 5:
                break

            open_price = float(candle[1])
            close_price = float(candle[4])

            if open_price == 0:
                break

            change_percent = abs((close_price - open_price) / open_price) * 100

            # Check if candle is bullish with sufficient change
            if change_percent >= min_change and close_price > open_price:
                bullish_count += 1
                turnover += float(candle[6])
                if bullish_end_price is None:  # First bullish candle found (closest to bearish)
                    bullish_end_price = close_price
                bullish_start_price = open_price  # This will end up being the FIRST bullish candle's open
            else:
                break  # Stop at first non-bullish candle

        # Verify bullish count is within configured range
        if not (config["min_bullish_consecutive"] <= bullish_count <= config["max_bullish_consecutive"]):
            return None

        # --- STEP 4: Validate pattern requirements ---
        # Requirement 1: More bullish than bearish candles
        if bullish_count <= bearish_count:
            return None

        # Requirement 2: Bearish drop <= max % of bullish gain
        if not all([bullish_start_price, bullish_end_price, first_bearish_open, last_bearish_close]):
            return None

        bullish_gain = bullish_end_price - bullish_start_price
        bearish_drop = first_bearish_open - last_bearish_close

        if bullish_gain <= 0:  # Must have positive gain
            return None

        bearish_drop_percent = (bearish_drop / bullish_gain) * 100

        # Enhanced debug logging
        if config["debug"]:
            latest_time = datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc)
            vienna_time = get_vienna_time(latest_time)

            # Calculate actual percentages for clarity
            bullish_gain_percent = ((bullish_end_price - bullish_start_price) / bullish_start_price) * 100
            bearish_drop_actual_percent = ((first_bearish_open - last_bearish_close) / first_bearish_open) * 100

            debug_info = f"""
[DEBUG] {pair} BF Pattern Analysis at {vienna_time.strftime('%H:%M:%S')}:
Pattern Structure: {bullish_count} bullish + {bearish_count} bearish candles
Bullish Phase: {bullish_start_price:.6f} -> {bullish_end_price:.6f} (gain: +{bullish_gain_percent:.2f}%)
Bearish Phase: {first_bearish_open:.6f} -> {last_bearish_close:.6f} (drop: -{bearish_drop_actual_percent:.2f}%)
Bullish Gain Amount: {bullish_gain:.6f}
Bearish Drop Amount: {bearish_drop:.6f}
Bearish Drop as % of Bullish Gain: {bearish_drop_percent:.2f}%
Max Allowed Drop: {config['max_bearish_drop_percent']}%
Pattern Valid: {bearish_drop_percent <= config['max_bearish_drop_percent']}"""

            log_message(debug_info)

        if bearish_drop_percent > config["max_bearish_drop_percent"]:
            return None

        # Check minimum cumulative turnover for BF alert
        if turnover < ALERT_CONFIG["alerts"]["bullish_flag"]["min_turnover"]:
            if config["debug"]:
                log_message(f"[BF DEBUG] {pair}: Turnover {turnover:,.2f} < required {ALERT_CONFIG['alerts']['bullish_flag']['min_turnover']}")
            return None

        # --- Pattern validated - create alert ---
        candle_time = datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc)
        vienna_time = get_vienna_time(candle_time)

        return {
            "pair": pair,
            "alert_type": "BF",
            # "candle_time_vienna": vienna_time.strftime("%H:%M:%S"),
            "candle_time_vienna": vienna_time.strftime("%H:%M"),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "bearish_drop_percent": round(bearish_drop_percent, 2),
            "turnover": turnover,
            "status": "valid"
        }

    except (ValueError, IndexError, TypeError, ZeroDivisionError) as e:
        log_message(f"Error processing BF pattern for {pair}: {str(e)}")
        return None

# ==================== BULLISH FLAG (BF) ALERT END =========================================

# ==================== BULLISH SERIES OF CANDLES (BS) ALERT BEGINN ==========================

def check_for_bs_alert(pair: str, candles: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """
    Check for Bullish Series (BS) alert pattern in candle data.

    A Bullish Series detects consecutive bullish candles where each candle:
    - Has close > open (bullish)
    - Meets minimum percentage change requirement
    - Optionally has increasing closes

    Args:
        pair: Trading pair symbol (e.g., 'BTCUSDT')
        candles: List of candle data [timestamp, open, high, low, close, volume, turnover]

    Returns:
        Dict with alert details if pattern found, None otherwise
    """
    # Skip if BS alerts are disabled
    if not ALERT_CONFIG["alerts"]["bullish_series"]["enabled"]:
        if ALERT_CONFIG["alerts"]["bullish_series"]["debug"]:
            log_message(f"[BS DEBUG] {pair}: BS alerts are disabled in config")
        return None

    # Validate input data
    if not candles:
        if ALERT_CONFIG["alerts"]["bullish_series"]["debug"]:
            log_message(f"[BS DEBUG] {pair}: No candles provided")
        return None

    if len(candles) < 2:
        if ALERT_CONFIG["alerts"]["bullish_series"]["debug"]:
            log_message(f"[BS DEBUG] {pair}: Not enough candles ({len(candles)} < 2)")
        return None

    # Get configuration parameters
    config = ALERT_CONFIG["alerts"]["bullish_series"]
    min_consecutive = config["min_consecutive_bullish"]
    max_consecutive = config["max_consecutive_bullish"]
    min_change_percent = config["min_candle_change_percent"]
    require_increasing = config["require_increasing_closes"]
    min_total_gain = config["min_total_series_gain"]
    debug_mode = config["debug"]

    # Validate configuration
    if min_consecutive <= 0 or max_consecutive < min_consecutive:
        log_message(
            f"[BS ERROR] {pair}: Invalid BS configuration - min_consecutive: {min_consecutive}, max_consecutive: {max_consecutive}")
        return None

    if debug_mode:
        log_message(f"[BS DEBUG] {pair}: Starting BS analysis with {len(candles)} candles")
        log_message(
            f"[BS DEBUG] {pair}: Config - min_consecutive: {min_consecutive}, max_consecutive: {max_consecutive}")
        log_message(
            f"[BS DEBUG] {pair}: Config - min_change_percent: {min_change_percent}%, require_increasing: {require_increasing}")
        log_message(f"[BS DEBUG] {pair}: Config - min_total_gain: {min_total_gain}%")

    try:
        # Count consecutive bullish candles working backwards from most recent
        consecutive_count = 0
        individual_gains = []
        series_start_open = None  # Open price of the FIRST bullish candle in the series
        series_end_close = None  # Close price of the LAST bullish candle in the series
        previous_close = None
        turnover = 0.0
        candle_details = []  # For debugging

        # Work backwards from the most recent candle
        for i in range(len(candles) - 1, -1, -1):
            candle = candles[i]

            # Validate candle structure
            if len(candle) < 7:  # Should have at least 7 elements including turnover
                if debug_mode:
                    log_message(
                        f"[BS DEBUG] {pair}: Invalid candle structure at index {i} - has {len(candle)} elements")
                break

            # Extract prices safely
            try:
                timestamp = int(candle[0])
                open_price = float(candle[1])
                high_price = float(candle[2])
                low_price = float(candle[3])
                close_price = float(candle[4])
                volume = float(candle[5])
                candle_turnover = float(candle[6])
            except (ValueError, TypeError) as e:
                if debug_mode:
                    log_message(f"[BS DEBUG] {pair}: Error parsing candle at index {i}: {e}")
                break

            # Validate prices
            if open_price <= 0 or close_price <= 0:
                if debug_mode:
                    log_message(
                        f"[BS DEBUG] {pair}: Invalid prices at index {i} - open: {open_price}, close: {close_price}")
                break

            # Calculate percentage change
            candle_change_percent = ((close_price - open_price) / open_price) * 100

            # Check if candle meets bullish criteria
            is_bullish = close_price > open_price
            meets_min_change = abs(candle_change_percent) >= min_change_percent  # Use absolute value for comparison

            # Store candle details for debugging
            candle_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            vienna_time = get_vienna_time(candle_time)
            candle_info = {
                "time": vienna_time.strftime('%H:%M:%S'),
                "open": open_price,
                "close": close_price,
                "change_percent": candle_change_percent,
                "is_bullish": is_bullish,
                "meets_min_change": meets_min_change,
                "turnover": candle_turnover
            }
            candle_details.append(candle_info)

            if debug_mode and consecutive_count < 5:  # Limit debug output to first 5 candles
                log_message(f"[BS DEBUG] {pair} candle #{consecutive_count + 1} at {candle_info['time']}: "
                            f"Open={open_price:.6f}, Close={close_price:.6f}, "
                            f"Change={candle_change_percent:.3f}%, Bullish={is_bullish}, "
                            f"Meets min change={meets_min_change}")

            # Stop if candle doesn't meet criteria
            if not (is_bullish and meets_min_change):
                if debug_mode:
                    reason = []
                    if not is_bullish:
                        reason.append("not bullish")
                    if not meets_min_change:
                        reason.append(f"change {candle_change_percent:.3f}% < min {min_change_percent}%")
                    log_message(
                        f"[BS DEBUG] {pair}: Series ended at candle #{consecutive_count + 1} - {', '.join(reason)}")
                break

            # Check increasing closes requirement if we have a previous close
            if require_increasing and previous_close is not None:
                if close_price <= previous_close:
                    if debug_mode:
                        log_message(f"[BS DEBUG] {pair}: Series ended at candle #{consecutive_count + 1} - "
                                    f"close {close_price:.6f} <= previous close {previous_close:.6f}")
                    break

            # Add to series
            consecutive_count += 1
            individual_gains.append(candle_change_percent)
            turnover += candle_turnover

            # Track series boundaries
            if series_end_close is None:  # First valid candle (most recent)
                series_end_close = close_price
            series_start_open = open_price  # Will be the earliest candle's open price

            previous_close = close_price

            # Stop at maximum limit to prevent excessive alerts
            if consecutive_count >= max_consecutive:
                if debug_mode:
                    log_message(f"[BS DEBUG] {pair}: Reached maximum consecutive limit ({max_consecutive})")
                break

        if debug_mode:
            log_message(
                f"[BS DEBUG] {pair}: Found {consecutive_count} consecutive bullish candles (min required: {min_consecutive})")

        # Check if series meets minimum requirements
        if consecutive_count < min_consecutive:
            if debug_mode:
                log_message(f"[BS DEBUG] {pair}: Series too short - {consecutive_count} < {min_consecutive}")
            return None

        # Calculate total series gain
        if series_start_open is None or series_end_close is None or series_start_open <= 0:
            if debug_mode:
                log_message(
                    f"[BS DEBUG] {pair}: Invalid series boundaries - start: {series_start_open}, end: {series_end_close}")
            return None

        total_gain_percent = ((series_end_close - series_start_open) / series_start_open) * 100

        if debug_mode:
            log_message(f"[BS DEBUG] {pair}: Total series gain calculation:")
            log_message(f"[BS DEBUG] {pair}: Start price (open): {series_start_open:.6f}")
            log_message(f"[BS DEBUG] {pair}: End price (close): {series_end_close:.6f}")
            log_message(f"[BS DEBUG] {pair}: Total gain: {total_gain_percent:.3f}% (min required: {min_total_gain}%)")

        # Check minimum total gain
        if total_gain_percent < min_total_gain:
            if debug_mode:
                log_message(f"[BS DEBUG] {pair}: Total gain {total_gain_percent:.3f}% < required {min_total_gain}%")
            return None

        # Check minimum cumulative turnover for BS alert
        if turnover < ALERT_CONFIG["alerts"]["bullish_series"]["min_turnover"]:
            if debug_mode:
                log_message(f"[BS DEBUG] {pair}: Turnover {turnover:,.2f} < required {ALERT_CONFIG['alerts']['bullish_series']['min_turnover']}")
            return None

        # Calculate additional metrics
        avg_candle_gain = sum(individual_gains) / len(individual_gains) if individual_gains else 0
        min_candle_gain = min(individual_gains) if individual_gains else 0
        max_candle_gain = max(individual_gains) if individual_gains else 0

        # Get alert timestamp from the most recent candle
        latest_candle = candles[-1]
        candle_time = datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc)
        vienna_time = get_vienna_time(candle_time)

        # Create alert dictionary
        alert = {
            "pair": pair,
            "alert_type": "BS",
            "candle_time_vienna": vienna_time.strftime("%H:%M"),
            "consecutive_count": consecutive_count,
            "total_gain_percent": round(total_gain_percent, 3),
            "avg_candle_gain": round(avg_candle_gain, 3),
            "min_candle_gain": round(min_candle_gain, 3),
            "max_candle_gain": round(max_candle_gain, 3),
            "series_start_price": series_start_open,
            "series_end_price": series_end_close,
            "individual_gains": [round(gain, 3) for gain in individual_gains],
            "turnover": turnover,
            "status": "valid"
        }

        if debug_mode:
            log_message(f"[BS SUCCESS] {pair}: Valid BS pattern detected!")
            log_message(f"[BS SUCCESS] {pair}: {consecutive_count} consecutive bullish candles")
            log_message(f"[BS SUCCESS] {pair}: Total gain: {total_gain_percent:.3f}%")
            log_message(f"[BS SUCCESS] {pair}: Average candle gain: {avg_candle_gain:.3f}%")
            log_message(f"[BS SUCCESS] {pair}: Individual gains: {[round(g, 2) for g in individual_gains]}")
            log_message(f"[BS SUCCESS] {pair}: Total turnover: {turnover:,.2f}")

        return alert

    except Exception as e:
        log_message(f"[BS ERROR] {pair}: Unexpected error processing BS pattern: {str(e)}")
        if debug_mode:
            import traceback
            log_message(f"[BS ERROR] {pair}: Traceback: {traceback.format_exc()}")
        return None

# ==================== BULLISH SERIES OF CANDLES (BS) ALERT END ============================

# ==================== BULLISH CONSECUTIVE OF CANDLES (BC) ALERT BEGIN =====================

def check_for_bc_alert(pair: str, candles: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """
    Bullish Consecutive (BC) Alert: 
    Detects consecutive non-bearish candles (close >= open), including dojis.
    No minimum price change required.
    """
    if not ALERT_CONFIG["alerts"]["bullish_consecutive"]["enabled"]:
        return None

    config = ALERT_CONFIG["alerts"]["bullish_consecutive"]
    min_consecutive = config["min_consecutive"]
    debug_mode = config["debug"]

    if len(candles) < min_consecutive:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Not enough candles ({len(candles)} < {min_consecutive})")
        return None

    consecutive_count = 0
    turnover = 0.0
    series_start_open = None
    series_end_close = None

    # Iterate backwards (from newest to oldest candle)
    for i in range(len(candles) - 1, -1, -1):
        candle = candles[i]
        open_price = float(candle[1])
        close_price = float(candle[4])

        # Check for non-bearish candle (close >= open)
        if close_price >= open_price:
            consecutive_count += 1
            turnover += float(candle[6])  # Add turnover

            # Track series boundaries
            if series_end_close is None:
                series_end_close = close_price
            series_start_open = open_price

            # Early exit if max consecutive reached
            if consecutive_count >= config["max_consecutive"]:
                break
        else:
            break  # Series ends on first bearish candle

    # Validate minimum consecutive count
    if consecutive_count < min_consecutive:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Only {consecutive_count}/{min_consecutive} consecutive candles")
        return None

    # Check minimum turnover
    if turnover < config["min_turnover"]:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Turnover {turnover} < {config['min_turnover']}")
        return None

    # Optional: Check for increasing closes
    if config["require_increasing_closes"]:
        closes = [float(c[4]) for c in candles[-consecutive_count:]]
        if any(closes[i] <= closes[i+1] for i in range(len(closes)-1)):
            if debug_mode:
                log_message(f"[BC DEBUG] {pair}: Closes not monotonically increasing")
            return None

    # Prepare alert
    latest_candle = candles[-1]
    candle_time = datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc)
    vienna_time = get_vienna_time(candle_time)

    return {
        "pair": pair,
        "alert_type": "BC",
        "candle_time_vienna": vienna_time.strftime("%H:%M"),
        "consecutive_count": consecutive_count,
        "turnover": turnover,
        "series_start_price": series_start_open,
        "series_end_price": series_end_close,
        "status": "valid"
    }def check_for_bc_alert(pair: str, candles: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """
    Bullish Consecutive (BC) Alert:
    Detects consecutive non-bearish candles (close >= open, including dojis).
    Calculates total % gain across the series.
    """
    config = ALERT_CONFIG["alerts"]["bullish_consecutive"]
    if not config["enabled"]:
        return None

    min_consecutive = config["min_consecutive"]
    debug_mode = config["debug"]

    if len(candles) < min_consecutive:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Not enough candles ({len(candles)} < {min_consecutive})")
        return None

    consecutive_count = 0
    turnover = 0.0
    series_start_open = None
    series_end_close = None

    # Iterate backwards (newest to oldest)
    for i in range(len(candles) - 1, -1, -1):
        candle = candles[i]
        open_price = float(candle[1])
        close_price = float(candle[4])

        # Check for non-bearish candle (close >= open)
        if close_price >= open_price:
            consecutive_count += 1
            turnover += float(candle[6])  # Add turnover

            # Track series boundaries
            if series_end_close is None:
                series_end_close = close_price  # Latest close in series
            series_start_open = open_price      # Earliest open in series

            # Exit if max consecutive reached
            if consecutive_count >= config["max_consecutive"]:
                break
        else:
            break  # Series ends on first bearish candle

    # Validate minimum consecutive count
    if consecutive_count < min_consecutive:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Only {consecutive_count}/{min_consecutive} candles")
        return None

    # Calculate total gain percentage
    if series_start_open is None or series_end_close is None or series_start_open <= 0:
        return None

    total_gain_percent = ((series_end_close - series_start_open) / series_start_open) * 100

    # Check minimum gain filter (if enabled)
    if total_gain_percent < config["min_total_gain_percent"]:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Gain {total_gain_percent:.2f}% < {config['min_total_gain_percent']}%")
        return None

    # Check minimum turnover
    if turnover < config["min_turnover"]:
        if debug_mode:
            log_message(f"[BC DEBUG] {pair}: Turnover {turnover:,.2f} < {config['min_turnover']}")
        return None

    # Prepare alert
    latest_candle = candles[-1]
    candle_time = datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc)
    vienna_time = get_vienna_time(candle_time)

    return {
        "pair": pair,
        "alert_type": "BC",
        "candle_time_vienna": vienna_time.strftime("%H:%M"),
        "consecutive_count": consecutive_count,
        "total_gain_percent": round(total_gain_percent, 2),  # Added % gain
        "turnover": turnover,
        "series_start_price": series_start_open,
        "series_end_price": series_end_close,
        "status": "valid"
    }

# ==================== BULLISH CONSECUTIVE OF CANDLES (BC) ALERT END =======================

def split_pair_symbol(pair: str) -> str:
    """Convert symbol pairs like BTCUSDT to BTC USDT"""
    if pair.endswith('USDT'):
        return f"{pair[:-4]}"
    return pair

async def update_round(usdt_pairs, round_number):
    log_message(f"=== Starting update round {round_number} ===")

    start_time = time.time()
    current_minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    log_message(f"Updating data for minute: {current_minute.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    alerts = []
    updated_stats = {}
    update_count = 0

    batch_size = 100
    batches = [usdt_pairs[i:i + batch_size] for i in range(0, len(usdt_pairs), batch_size)]

    for batch_index, batch in enumerate(batches):
        log_message(f"Processing batch {batch_index + 1}/{len(batches)} ({len(batch)} pairs)")
        tasks = [update_pair_data(pair, current_minute) for pair in batch]
        results = await asyncio.gather(*tasks)

        for i, (success, stats) in enumerate(results):
            pair = batch[i]
            if success:
                try:
                    if pair in pair_candles and len(pair_candles[pair]) >= MOVING_AVERAGES_CONFIG["window"]:
                        volume_ma, price_length_ma, total_range_ma = calculate_moving_averages(pair_candles[pair])
                        if None in (volume_ma, price_length_ma, total_range_ma):
                            continue

                        stats.update({
                            "volume_ma": volume_ma,
                            "price_length_ma": price_length_ma,
                            "total_range_ma": total_range_ma,
                            "candle_time": datetime.fromtimestamp(pair_candles[pair][-1][0]/1000, tz=timezone.utc).strftime('%H:%M:%S')
                        })

                        updated_stats[pair] = stats
                        update_count += 1

                        # Check alerts
                        if ALERT_CONFIG["alerts"]["bullish_long"]["enabled"]:
                            latest_candle = pair_candles[pair][-1]
                            bl_alert = check_for_long_bullish_candle(pair, latest_candle)
                            if bl_alert:
                                alerts.append(bl_alert)
                                log_message(
                                    f"{bl_alert['candle_time_vienna']} {split_pair_symbol(pair)} BL ({bl_alert['price_change_percent']:.2f}%)/{bl_alert['turnover']:,.2f}",
                                    is_alert=True
                                )

                        if ALERT_CONFIG["alerts"]["bullish_marubozo"]["enabled"]:
                            latest_candle = pair_candles[pair][-1]
                            bm_alert = check_for_marubozo_bullish_candle(pair, latest_candle)
                            if bm_alert:
                                alerts.append(bm_alert)
                                log_message(
                                    f"{bm_alert['candle_time_vienna']} {split_pair_symbol(pair)} BM ({bm_alert['price_change_percent']:.2f}%)/{bm_alert['turnover']:,.2f}",
                                    is_alert=True
                                )

                        if ALERT_CONFIG["alerts"]["bullish_flag"]["enabled"]:
                            bf_alert = check_for_bf_alert(pair, pair_candles[pair])
                            if bf_alert:
                                alerts.append(bf_alert)
                                log_message(
                                    f"{bf_alert['candle_time_vienna']} {split_pair_symbol(pair)} BF ({bf_alert['bullish_count']}+{bf_alert['bearish_count']})/{bf_alert['turnover']:,.2f}",
                                    is_alert=True
                                )

                        if ALERT_CONFIG["alerts"]["bullish_series"]["enabled"]:
                            bs_alert = check_for_bs_alert(pair, pair_candles[pair])
                            if bs_alert:
                                alerts.append(bs_alert)
                                log_message(
                                    f"{bs_alert['candle_time_vienna']} {split_pair_symbol(pair)} BS({bs_alert['consecutive_count']}) +{bs_alert['total_gain_percent']:.2f}%/{bs_alert['turnover']:,.2f}",
                                    is_alert=True
                                )

                         if ALERT_CONFIG["alerts"]["bullish_consecutive"]["enabled"]:
                             bc_alert = check_for_bc_alert(pair, pair_candles[pair])
                             if bc_alert:
                                 alerts.append(bc_alert)
                                 log_message(
                                     f"{bc_alert['candle_time_vienna']} {split_pair_symbol(pair)} BC({bc_alert['consecutive_count']})/{bc_alert['turnover']:,.2f}",
                                     is_alert=True
                                 )

                except Exception as e:
                    log_message(f"Error processing pair {pair}: {str(e)}")
                    continue

            await asyncio.sleep(0.02)

    save_candles_to_csv(pair_candles, f"update_{round_number}")

    end_time = time.time()
    execution_time = end_time - start_time

    log_message(f"Successfully updated pairs: {update_count}/{len(usdt_pairs)}")
    log_message(f"Alerts found: {len(alerts)}")
    log_message(f"Update execution time: {execution_time:.2f} seconds")

    return updated_stats, alerts

async def wait_until_next_minute_plus_5():
    now = datetime.now(timezone.utc)
    seconds_to_next_minute = 60 - now.second
    wait_seconds = seconds_to_next_minute + 5
    log_message(f"Current time: {now.strftime('%H:%M:%S')} UTC")
    log_message(f"Waiting {wait_seconds} seconds until {(now + timedelta(seconds=wait_seconds)).strftime('%H:%M:%S')} UTC")
    await asyncio.sleep(wait_seconds)

async def fetch_ohlc_and_update():
    try:
        setup_logging(ALERT_CONFIG["enable_logging"])
        log_message("Starting Binance Scanner")
        log_message("First waiting for next minute mark...")
        await wait_until_next_minute_plus_5()

        log_message("\n=== PHASE 1: FETCHING INITIAL DATA ===")
        usdt_pairs = await fetch_usdt_pairs_with_usdc_counterparts()
        log_message(f"Found {len(usdt_pairs)} USDT pairs with USDC counterparts")

        await initial_fetch(usdt_pairs)

        log_message("\n=== PHASE 2: STARTING LIVE UPDATES ===")
        log_message(f"Will fetch {CANDLE_HISTORY_CONFIG['update_limit']} new candles each minute")
        round_number = 1

        while True:
            log_message(f"\n=== PREPARING ROUND {round_number} ===")
            await wait_until_next_minute_plus_5()

            log_message(f"Starting update round {round_number}")
            updated_stats, alerts = await update_round(usdt_pairs, round_number)

            round_number += 1

    except Exception as e:
        log_message(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            await session.close()
        if log_file:
            log_file.close()
        log_message("Scanner stopped")

async def main():
    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)

    log_message("Starting Binance Real-time Scanner (Spot Market)...")
    log_message("Only scanning USDT pairs that have USDC counterparts")
    log_message("Waiting for first data collection at the next minute mark...")
    await fetch_ohlc_and_update()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
