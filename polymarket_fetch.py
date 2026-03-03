import csv
import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://gamma-api.polymarket.com/markets"
TRADES_URL = "https://data-api.polymarket.com/trades"
OUTPUT_PATH = "/Users/tangyuchen/Desktop/cornell/26Spring/SYSEN5380/project/polymarket_markets.csv"
TRADES_PAGE_LIMIT = 500
TRADES_MAX_PAGES = 1
MAX_WORKERS = 8


def to_iso_utc_day_start(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")


def to_iso_utc_day_end(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%dT23:59:59Z")


def parse_iso_utc(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_json_array(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return []
    return []


def fetch_json(url, params, retries=5):
    full_url = f"{url}?{urlencode(params)}"
    request = Request(full_url, headers={"User-Agent": "python-urllib"})
    for attempt in range(retries):
        try:
            with urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                if attempt == retries - 1:
                    raise
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except (TimeoutError, socket.timeout, URLError, OSError):
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def fetch_markets_page(params):
    try:
        return fetch_json(BASE_URL, params)
    except HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = ""
        if detail:
            raise SystemExit(f"HTTP error: {e.code} {e.reason}\n{detail}") from e
        raise SystemExit(f"HTTP error: {e.code} {e.reason}") from e
    except URLError as e:
        raise SystemExit(f"Network error: {e.reason}") from e
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON response: {e}") from e


def fetch_trades_page(params):
    try:
        return fetch_json(TRADES_URL, params)
    except HTTPError as e:
        # Some markets may have no accessible trade pages for deep offsets.
        if e.code in (400, 404):
            return []
        raise
    except (URLError, json.JSONDecodeError):
        return []


def get_resolved_markets(start_date, end_date, category=None, limit=100):
    all_markets = []
    offset = 0
    start_dt = to_iso_utc_day_start(start_date)
    end_dt = to_iso_utc_day_end(end_date)

    while True:
        params = {
            "closed": True,
            "end_date_min": start_dt,
            "end_date_max": end_dt,
            "limit": limit,
            "offset": offset,
            "order": "endDate",
            "ascending": False,
        }
        if category:
            params["category"] = category

        page = fetch_markets_page(params)
        if not page:
            break

        all_markets.extend(page)
        offset += limit
        print(f"Fetched {len(all_markets)} markets...")

        if len(page) < limit:
            break

    return all_markets


def parse_final_outcome_binary(market):
    outcomes = [str(x).strip().lower() for x in parse_json_array(market.get("outcomes"))]
    if len(outcomes) != 2 or "yes" not in outcomes or "no" not in outcomes:
        return None

    prices_raw = parse_json_array(market.get("outcomePrices"))
    if len(prices_raw) != 2:
        return None
    try:
        prices = [float(x) for x in prices_raw]
    except (TypeError, ValueError):
        return None

    yes_index = outcomes.index("yes")
    no_index = outcomes.index("no")
    yes_price = prices[yes_index]
    no_price = prices[no_index]

    if yes_price == 1.0 and no_price == 0.0:
        return 1
    if yes_price == 0.0 and no_price == 1.0:
        return 0
    return None


def is_excluded_resolution(market):
    text = " ".join(
        [
            str(market.get("resolution", "")),
            str(market.get("winner", "")),
            str(market.get("umaResolutionStatus", "")),
        ]
    ).lower()
    blocked = ("invalid", "ambiguous", "cancel", "void")
    return any(x in text for x in blocked)


def get_prob_day_minus_1(
    condition_id,
    end_date_str,
    page_limit=TRADES_PAGE_LIMIT,
    max_pages=TRADES_MAX_PAGES,
):
    end_dt = parse_iso_utc(end_date_str)
    target_ts = int((end_dt - timedelta(days=1)).timestamp())
    offset = 0

    for _ in range(max_pages):
        params = {
            "market": condition_id,
            "limit": page_limit,
            "offset": offset,
        }
        trades = fetch_trades_page(params)
        if not isinstance(trades, list) or not trades:
            break

        # API returns trades in reverse-chronological order for each market.
        min_ts = min(int(t.get("timestamp", 0)) for t in trades if "timestamp" in t)
        if min_ts > target_ts:
            offset += page_limit
            continue

        candidate = None
        for t in trades:
            ts = int(t.get("timestamp", 0))
            if ts > target_ts:
                continue
            outcome = str(t.get("outcome", "")).strip().lower()
            try:
                price = float(t.get("price"))
            except (TypeError, ValueError):
                continue

            if outcome == "yes":
                yes_prob = price
            elif outcome == "no":
                yes_prob = 1.0 - price
            else:
                continue
            candidate = (yes_prob, ts)
            break

        if candidate:
            yes_prob, ts = candidate
            yes_prob = max(0.0, min(1.0, yes_prob))
            ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            return yes_prob, ts_iso

        offset += page_limit

    return None, None


def build_research_samples(markets):
    rows = []

    candidates = []
    for m in markets:
        if is_excluded_resolution(m):
            continue

        final_outcome = parse_final_outcome_binary(m)
        if final_outcome is None:
            continue

        end_date = m.get("endDate")
        if not end_date:
            continue

        condition_id = m.get("conditionId")
        if not condition_id:
            continue

        candidates.append((m, final_outcome, condition_id, end_date))

    total = len(candidates)
    print(f"Eligible markets after filters: {total}")

    def process_one(item):
        m, final_outcome, condition_id, end_date = item
        pred_prob, pred_prob_ts = get_prob_day_minus_1(condition_id, end_date)
        if pred_prob is None:
            return None
        return {
            "market_id": m.get("id"),
            "question": m.get("question", ""),
            "category": m.get("category"),
            "startDate": m.get("startDate"),
            "endDate": end_date,
            "pred_prob_day_minus_1": pred_prob,
            "pred_prob_timestamp_utc": pred_prob_ts,
            "final_outcome_yes": final_outcome,
            "volume": m.get("volume"),
            "liquidity": m.get("liquidity"),
            "closed": m.get("closed"),
            "resolution": m.get("resolution"),
            "winner": m.get("winner"),
        }

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_one, item) for item in candidates]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                rows.append(result)
            done += 1
            if done % 100 == 0:
                print(f"Processed {done}/{total} eligible markets, kept {len(rows)} samples...")

    rows.sort(key=lambda x: int(x["market_id"]) if str(x["market_id"]).isdigit() else str(x["market_id"]))
    return rows


def save_csv(rows, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        columns = [
            "market_id",
            "question",
            "category",
            "startDate",
            "endDate",
            "pred_prob_day_minus_1",
            "pred_prob_timestamp_utc",
            "final_outcome_yes",
            "volume",
            "liquidity",
            "closed",
            "resolution",
            "winner",
        ]
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    markets = get_resolved_markets(
        start_date="2023-01-01",
        end_date="2024-12-31",
        category=None,
        limit=100,
    )
    samples = build_research_samples(markets)
    save_csv(samples, OUTPUT_PATH)
    print(f"Total closed markets fetched: {len(markets)}")
    print(f"Research samples kept: {len(samples)}")
    print(f"Saved CSV: {OUTPUT_PATH}")
