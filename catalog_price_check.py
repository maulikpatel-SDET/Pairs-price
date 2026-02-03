# import requests
# from datetime import datetime
# import pytz
# import concurrent.futures


# def get_current_datetime_ist():
#     return datetime.now(pytz.timezone("Asia/Kolkata"))


# def convert_date_time_with_timezone(date_time):
#     datetime_object = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
#     return datetime_object.astimezone(pytz.timezone("Asia/Kolkata"))


# def post_api_data_from_supra_data_dashboard(payload):
#     url = "https://prod-bff-dashboard.supra.com/graphql"
#     headers = {"Content-Type": "application/json"}
#     return requests.post(url, json=payload, headers=headers, timeout=300)


# def get_catalog_page_pairs_all_pairs_name():
#     payload = {
#         "operationName": "GetCatalog",
#         "variables": {
#             "input": {
#                 "doraType": "2",
#                 "instrumentTypeIds": ["1", "2", "3", "5", "7"],
#                 "providerIds": "20",
#                 "first": 1200,
#                 "page": 1,
#                 "pageSize": 1200
#             }
#         },
#         "query": """
#         query GetCatalog($input: CatalogInput) {
#           catalog(input: $input) {
#             relatedFilters {
#               instruments {
#                 tradingPair
#                 instrumentTypeId
#               }
#             }
#           }
#         }
#         """
#     }

#     response = post_api_data_from_supra_data_dashboard(payload)
#     if response.status_code == 200:
#         return response.json()["data"]["catalog"]["relatedFilters"]["instruments"]

#     return None


# def get_catalog_trading_pair_prices(pair_name):
#     payload = {
#         "operationName": "GetCatalogTradingPairPrices",
#         "variables": {
#             "input": {
#                 "instrumentPairDisplayName": pair_name,
#                 "providerId": "20",
#                 "limit": 1,
#                 "doraType": "2"
#             }
#         },
#         "query": """
#         query GetCatalogTradingPairPrices($input: CatalogTradingPairPricesAndGraphInput) {
#           catalogTradingPairPrices(input: $input) {
#             timestamp
#           }
#         }
#         """
#     }

#     response = post_api_data_from_supra_data_dashboard(payload)
#     if response.status_code == 200:
#         data = response.json()["data"]["catalogTradingPairPrices"]
#         return data[0]["timestamp"] if data else None

#     return None


# def fetch_price(pair):
#     return pair, get_catalog_trading_pair_prices(pair)

# def test_CER_1769_catalog_details_page_check_pairs_prices():

#     failed_pairs = []

#     instruments = get_catalog_page_pairs_all_pairs_name()
#     assert instruments, "Failed to fetch catalog instruments"

#     pair_type_map = {
#         item["tradingPair"]: item["instrumentTypeId"]
#         for item in instruments
#     }

#     trading_pairs = list(pair_type_map.keys())

#     current_datetime = get_current_datetime_ist()

#     with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
#         futures = [executor.submit(fetch_price, pair) for pair in trading_pairs]

#         for future in concurrent.futures.as_completed(futures):
#             trading_pair, timestamp = future.result()

#             if not timestamp:
#                 failed_pairs.append(trading_pair)
#                 continue

#             node_datetime = convert_date_time_with_timezone(timestamp)

#             instrument_type_id = pair_type_map.get(trading_pair)
#             allowed_minutes = 360 if instrument_type_id == "7" else 10

#             delta_minutes = abs(
#                 (current_datetime - node_datetime).total_seconds()
#             ) / 60


#             if delta_minutes > allowed_minutes:
#                 failed_pairs.append(trading_pair)

#     if failed_pairs:
#         print(f"Price feed not updated: {set(failed_pairs)}")


# if __name__ == "__main__":
#     test_CER_1769_catalog_details_page_check_pairs_prices()

import requests
from datetime import datetime
import pytz
import os
import concurrent.futures

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"
# SLACK_WEBHOOK_URL:https://hooks.slack.com/services/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
# SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL") 


def send_slack_message(message, success=True):
    emoji = ":white_check_mark:" if success else ":x:"
    payload = {
        "text": f"{emoji} *Catalog Price Feed Check*\n{message}"
    }
    requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)


def get_current_datetime_ist():
    return datetime.now(pytz.timezone("Asia/Kolkata"))


def convert_date_time_with_timezone(date_time):
    datetime_object = datetime.fromisoformat(date_time.replace("Z", "+00:00"))
    return datetime_object.astimezone(pytz.timezone("Asia/Kolkata"))


def post_api_data_from_supra_data_dashboard(payload):
    url = "https://prod-bff-dashboard.supra.com/graphql"
    # url = "https://qa-api.cerberus.supra.com/graphql"
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

    return []


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

    return None


def fetch_price(pair):
    return pair, get_catalog_trading_pair_prices(pair)


# ================= MAIN TEST =================
def test_CER_1769_catalog_details_page_check_pairs_prices():

    failed_pairs = []

    instruments = get_catalog_page_pairs_all_pairs_name()
    if not instruments:
        send_slack_message(
            "*Status:* FAILED\nCatalog instruments API failed",
            success=False
        )
        return

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

            # ❌ ERROR CONDITION 1: No timestamp
            if not timestamp:
                failed_pairs.append(f"{trading_pair} (no timestamp)")
                continue

            node_datetime = convert_date_time_with_timezone(timestamp)
            instrument_type_id = pair_type_map.get(trading_pair)
            allowed_minutes = 360 if instrument_type_id == "7" else 10

            delta_minutes = abs(
                (current_datetime - node_datetime).total_seconds()
            ) / 60

            # ❌ ERROR CONDITION 2: Price too old
            if delta_minutes > allowed_minutes:
                failed_pairs.append(
                    f"{trading_pair} ({int(delta_minutes)} mins old)"
                )

    # ================= SLACK RESULT =================
    if failed_pairs:   # 🔴 THIS CONDITION SENDS ERROR TO SLACK
        message = (
            f"*Status:* FAILED\n"
            f"*Failed Pairs:* {len(failed_pairs)}\n"
            f"*Details:*\n```" + "\n".join(failed_pairs) + "```"
        )
        send_slack_message(message, success=False)
    else:
        message = (
            "*Status:* PASSED\n"
            f"*Checked Pairs:* {len(trading_pairs)}\n"
            "All price feeds are updating correctly."
        )
        send_slack_message(message, success=True)


# ================= RUN =================
if __name__ == "__main__":
    test_CER_1769_catalog_details_page_check_pairs_prices()
