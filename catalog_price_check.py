import requests
from datetime import datetime
import pytz
import concurrent.futures
import traceback

TELEGRAM_BOT_TOKEN = "8202787356:AAHf5MKKGJhjkOslwShiFlC8YiIMJLtwb2o"
TELEGRAM_CHAT_ID = "6445351835"

# 8202787356:AAHf5MKKGJhjkOslwShiFlC8YiIMJLtwb2o
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)


def get_current_datetime_ist():
    return datetime.now(pytz.timezone("Asia/Kolkata"))


def convert_date_time_with_timezone(date_time):
    datetime_object = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
    return datetime_object.astimezone(pytz.timezone("Asia/Kolkata"))


def post_api_data_from_supra_data_dashboard(payload):
    url = "https://prod-bff-dashboard.supra.com/graphql"
    headers = {"Content-Type": "application/json"}
    return requests.post(url, json=payload, headers=headers, timeout=300)


def get_catalog_page_pairs_all_pairs_name():
    payload = {
        "operationName": "GetCatalog",
        "variables": {
            "input": {
                "doraType": "2",
                "instrumentTypeIds": ["1", "2", "3", "5", "7"],
                "providerIds": "20",
                "first": 1200,
                "page": 1,
                "pageSize": 1200
            }
        },
        "query": """
        query GetCatalog($input: CatalogInput) {
          catalog(input: $input) {
            relatedFilters {
              instruments {
                tradingPair
                instrumentTypeId
              }
            }
          }
        }
        """
    }

    response = post_api_data_from_supra_data_dashboard(payload)
    if response.status_code == 200:
        return response.json()["data"]["catalog"]["relatedFilters"]["instruments"]

    send_telegram_message("❌ Failed to fetch catalog instruments API")
    return None


def get_catalog_trading_pair_prices(pair_name):
    payload = {
        "operationName": "GetCatalogTradingPairPrices",
        "variables": {
            "input": {
                "instrumentPairDisplayName": pair_name,
                "providerId": "20",
                "limit": 1,
                "doraType": "2"
            }
        },
        "query": """
        query GetCatalogTradingPairPrices($input: CatalogTradingPairPricesAndGraphInput) {
          catalogTradingPairPrices(input: $input) {
            timestamp
          }
        }
        """
    }

    response = post_api_data_from_supra_data_dashboard(payload)
    if response.status_code == 200:
        data = response.json()["data"]["catalogTradingPairPrices"]
        return data[0]["timestamp"] if data else None

    send_telegram_message(f"⚠️ API failed for trading pair: {pair_name}")
    return None


def fetch_price(pair):
    return pair, get_catalog_trading_pair_prices(pair)


def test_CER_1769_catalog_details_page_check_pairs_prices():
    try:
        failed_pairs = []

        instruments = get_catalog_page_pairs_all_pairs_name()
        if not instruments:
            raise Exception("No instruments found")

        pair_type_map = {
            item["tradingPair"]: item["instrumentTypeId"]
            for item in instruments
        }

        trading_pairs = list(pair_type_map.keys())
        current_datetime = get_current_datetime_ist()

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(fetch_price, pair) for pair in trading_pairs]

            for future in concurrent.futures.as_completed(futures):
                trading_pair, timestamp = future.result()

                if not timestamp:
                    failed_pairs.append(trading_pair)
                    continue

                node_datetime = convert_date_time_with_timezone(timestamp)
                instrument_type_id = pair_type_map.get(trading_pair)
                allowed_minutes = 360 if instrument_type_id == "7" else 10

                delta_minutes = abs(
                    (current_datetime - node_datetime).total_seconds()
                ) / 60

                if delta_minutes > allowed_minutes:
                    failed_pairs.append(trading_pair)

        if failed_pairs:
            message = (
                "🚨 <b>Price Feed Delay Alert</b>\n\n"
                f"<b>Failed Pairs:</b>\n{', '.join(set(failed_pairs))}"
            )
            send_telegram_message(message)
        else:
            send_telegram_message("✅ All trading pairs are updating correctly")

    except Exception as e:
        error_message = (
            "🔥 <b>Script Execution Failed</b>\n\n"
            f"<pre>{traceback.format_exc()}</pre>"
        )
        send_telegram_message(error_message)


if __name__ == "__main__":
    test_CER_1769_catalog_details_page_check_pairs_prices()
