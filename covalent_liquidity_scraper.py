import copy
import json


from constants import *
import requests
import csv
import pandas
import numpy as np
import time
from datetime import datetime, timezone, timedelta
from dateutil import parser
from multiprocessing import Process, Queue
from queue import Empty
from collections import namedtuple
import traceback
import aiohttp
import asyncio

start_time = parser.parse("2021-11-01 00:00:00+00:00")
end_time = start_time + timedelta(days=(datetime.now(tz=timezone.utc) - start_time).days)
# end_time = start_time + timedelta(days=(datetime.now(tz=timezone.utc) - start_time).days + 1)
nb_processes = 20
possible_assets = ["OHM", "wsOHM", "gOHM", "OHMv1"]


# function to use requests.post to make an API call to the subgraph url
def run_query(query_type, **kwargs):
    query = ""
    if query_type == "log_events":
        query = COVALENT_ENDPOINT + str(kwargs["chain"]) + "/events/address/" + kwargs["address"] + \
                f"/?&starting-block={kwargs['starting_block']}&ending-block={kwargs['end_block']}" \
                f"&page-size={kwargs['page_size'] if 'page_size' in kwargs else 10000}" \
                f"&page-number={kwargs['page_nb'] if 'page_nb' in kwargs else 0}" \
                f"&key={API_KEY}"
    elif query_type == "get_block_for_date":
        query = COVALENT_ENDPOINT + str(kwargs["chain"]) + f"/block_v2/{kwargs['start_date']}/{kwargs['end_date']}" + \
                f"/?&key=" + API_KEY
    elif query_type == "get_token_balances":
        query = COVALENT_ENDPOINT + str(kwargs["chain"]) + "/address/" + kwargs[
            "address"] + "/balances_v2/?quote-currency=USD&format=JSON&nft=false&no-nft-fetch=true&key=" + API_KEY
    elif query_type == "get_token_holders":
        query = COVALENT_ENDPOINT + f"{kwargs['chain']}/tokens/{kwargs['address']}/token_holders/?key={API_KEY}&block-height={kwargs['block']}&page-size=10000&page-number={kwargs['page_nb']}"
    elif query_type == "refresh_prices":
        query = COVALENT_ENDPOINT + "pricing/tickers/?quote-currency=USD&format=JSON&tickers=" + "".join(
            [ticker + "," for ticker in kwargs["token_price"]])[:-1] + "&key=" + API_KEY
    elif query_type == "get_transactions":
        query = COVALENT_ENDPOINT + str(kwargs["chain"]) + "/address/" + kwargs[
            "address"] + "/transactions_v2/?" + f"quote-currency=USD&format=JSON&block-signed-at-asc={kwargs['ascending']}&" +\
                "page-size=5000&page-number=" + str(kwargs["page_number"]) + "&key=" + API_KEY
    elif query_type == "get_price":
        query = COVALENT_ENDPOINT + f"pricing/historical_by_addresses_v2/{kwargs['chain']}/USD/{kwargs['address']}/?quote-currency=USD&format=JSON&from={kwargs['start_date']}&to={kwargs['end_date']}&key={API_KEY}"
    elif query_type == "refresh_historical_prices":
        ticker = kwargs["ticker"]
        start_day = kwargs["start_day"]
        end_day = kwargs["end_day"]
        query = COVALENT_ENDPOINT + f"pricing/historical/USD/{ticker}/?quote-currency=USD&format=JSON&from={start_day}&to={end_day}&page-size=1000&key={API_KEY}"
    else:
        assert False, f"Query type {query_type} is not supported."

    print(f"Query is {query}")
    assert query != ""

    # endpoint where you are making the request
    while True:
        try:
            request = requests.get(query)
        except requests.exceptions.ChunkedEncodingError:
            print(f"Got a ChunkedEncodingError for query {query}, sleeping 15s and retrying")
            time.sleep(15)
            continue
        except:
            print(f"Got an unknown exception. Retrying in 20s.")
            print(traceback.format_exc())
            time.sleep(20)
        else:
            if request.status_code == 200:
                return request.json()
            elif request.status_code == 504:
                print(f"Got a 504 error - Sleeping for 30s for query {query}")
                time.sleep(30)
                continue
            else:
                try:
                    message = request.json()
                    if "error_message" in message:
                        print(f"Error message: {message['error_message']}")
                except:
                    print(f"There was an exception when jsonifying for request {request}")
                print('Query failed. return code is {}. {}. Sleeping for 10s'.format(request.status_code, query))
                time.sleep(10)


async def get(url, session):
    while True:
        try:
            async with session.get(url=url) as response:
                resp = await response.json()
                if "error" in resp and resp["error"]:
                    print(f"Error in query: {resp['error_message'] if 'error_message' in resp else ''} - Retrying in 5 seconds")
                    time.sleep(5)
                    continue

                print("Successfully got url {} with resp of length {}.".format(url, len(resp)))
                return resp
        except Exception as e:
            print("Unable to get url {} due to {}.".format(url, e.__class__))
            print("Sleeping 5 seconds ...")
            time.sleep(5)


async def get_asynch_urls(urls):
    async with aiohttp.ClientSession() as session:
        ret = await asyncio.gather(*[get(url, session) for url in urls])
    print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
    return ret


def querier(_pool_id, _nb_processes, _queue_to_joiner):
    for pool in pools:

        # We get all the transactions for this pool
        current_page = _pool_id
        should_ask_next_page = True
        while should_ask_next_page:
            all_transactions = run_query("get_transactions", chain=chain_dict[pool.chain], address=pool.address,
                                         page_number=current_page, ascending=True)

            if not all_transactions['data']['pagination']['has_more']:
                should_ask_next_page = False

            if len(all_transactions["data"]["items"]) != 0:
                first_tx_page_date = parser.parse(all_transactions["data"]["items"][0]["block_signed_at"])

                if first_tx_page_date > end_time:
                    all_transactions['data']['pagination']['has_more'] = False
                    should_ask_next_page = False

            # Send transactions if in [start_date, end_date] or < start date but uniswapV3 or empty
            _queue_to_joiner.put(all_transactions)

            current_page += _nb_processes


def joiner(_queues_queriers_to_joiner, _queue_joiner_to_main):
    PageInfo = namedtuple("PageInfo", ["address", "pageNb"])

    current_pool_index = 0
    current_pool = pools[current_pool_index]
    print(f"Joiner starting on pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}.")

    current_page = 0
    unsent_pages = []
    unsent_pageInfos = []

    while current_pool_index < len(pools):
        for queue in _queues_queriers_to_joiner:
            try:
                next_transaction_page = queue.get(block=False)
            except Empty:
                continue
            else:

                unsent_pageInfos.append(PageInfo(next_transaction_page["data"]["address"], next_transaction_page["data"]["pagination"]["page_number"]))
                unsent_pages.append(next_transaction_page)

        pages_idx_to_remove = []
        for i, pageInfo in enumerate(unsent_pageInfos):
            page_address, page_nb = pageInfo.address, pageInfo.pageNb

            if (page_address == current_pool.address) and (current_page == page_nb):
                to_send = copy.deepcopy(unsent_pages[i])
                pages_idx_to_remove.append(i)

                if not (len(to_send["data"]["items"]) == 0):
                # \
                #         or (parser.parse(to_send["data"]["items"][-1]["block_signed_at"]) < start_time \
                #         and current_pool.exchange != "UniswapV3")):
                    # Only sending if not empty page or after start_time (except if v3) or only one page
                    _queue_joiner_to_main.put(to_send)
                    print(f"Joiner sent page page {page_nb} for pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}")

                current_page += 1
                if (not to_send["data"]["pagination"]["has_more"]):
                    current_pool_index += 1
                    print(f"Joiner finished sending {current_page} pages for pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}.")
                    if current_pool_index < len(pools):
                        current_pool = pools[current_pool_index]
                        current_page = 0
                        print(f"Joiner switching to pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}.")

        for pageIdx in pages_idx_to_remove[::-1]:
            del unsent_pages[pageIdx]
            del unsent_pageInfos[pageIdx]

    _queue_joiner_to_main.put("done")


def main(_queue_joiner_to_main):
    path_to_liquidity_csv = SAVE_TO_FOLDER / "temp.csv"
    if path_to_liquidity_csv.exists():
        path_to_liquidity_csv.unlink()

    path_to_decimals = SAVE_TO_FOLDER / "decimals.json"
    path_to_price = SAVE_TO_FOLDER / "prices"
    if not path_to_price.exists():
        path_to_price.mkdir()


    all_tokens_decimals = {}
    if path_to_decimals.exists():
        with open(path_to_decimals, "r") as f:
            all_tokens_decimals = json.load(f)
    print(f"Loaded token decimals from {path_to_decimals}")
    print(f"Token decimals before refresh: {all_tokens_decimals}")

    already_counted_transactions = {}

    to_update_tokens_price = {
        "MIM": {}
        , "DAI": {}
        , "FRAX": {}
        , "LUSD": {}
        , "USDC": {}
        , "WETH": {}
        , "ETH": {}
        , "USDT": {}
        , "MATIC": {}
        , "BTC": {}
    }

    tokens_price = {}

    if path_to_price.exists():
        for price_json_path in path_to_price.iterdir():
            if ".json" in price_json_path.name:
                ticker = ".".join(price_json_path.name.split(".")[:-1])
                print(f"Loading prices from {price_json_path} ...")
                with open(price_json_path, "r") as f:
                    loaded_token_price = json.load(f)

                if ticker not in tokens_price:
                    tokens_price[ticker] = {}
                for str_day in loaded_token_price:
                    if str_day not in tokens_price[ticker]:
                        print(f"Loaded price {loaded_token_price[str_day]} for {ticker} on {str_day}")
                        tokens_price[ticker][str_day] = loaded_token_price[str_day]

        print("Finished loading prices.")

    all_str_days = [(start_time + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_time - start_time).days)]
    # We get daily historical prices for tokens in tokens_price
    for ticker in to_update_tokens_price:
        if ticker not in tokens_price:
            tokens_price[ticker] = {}
        should_update = False
        for str_day in all_str_days:
            if str_day not in tokens_price[ticker]:
                should_update = True
                print(f"Refreshing prices for asset {ticker} because didn't have price for date {str_day}")
                break

        if should_update:
            if ticker in ["DAI", "MIM", "LUSD", "FRAX", "USDT", "USDC"]:
                for str_day in all_str_days:
                    tokens_price[ticker][str_day] = 1
                    print(f"Refreshing price of {ticker} to be {1} on {str_day}")

            else:
                historical_prices_json = run_query("refresh_historical_prices",
                                                   ticker=ticker,
                                                   start_day=start_time.strftime("%Y-%m-%d"),
                                                   end_day=end_time.strftime("%Y-%m-%d"),
                                                   )
                for day_json in historical_prices_json["data"]["prices"]:
                    print(f"Refreshing price of {ticker} to be {day_json['price']} on {day_json['date']}")
                    tokens_price[ticker][day_json["date"]] = day_json["price"]

    for ticker in tokens_price:
        print(f"Saving prices of {ticker} to {path_to_price}")
        with open(path_to_price / f"{ticker}.json", mode='w') as f:
            json.dump(tokens_price[ticker], f, indent=2)

    # We get the price of avax
    historical_prices_json = run_query("get_price", chain=43114, address='0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
                           start_date=start_time.strftime("%Y-%m-%d"), end_date=end_time.strftime("%Y-%m-%d"))
    tokens_price['AVAX'] = {}
    tokens_price['WAVAX'] = {}
    for day_json in historical_prices_json["data"][0]["prices"]:
        print(f"Refreshing price of WAVAX and AVAX to be {day_json['price']} on {day_json['date']}")
        tokens_price['AVAX'][day_json["date"]] = day_json["price"]
        tokens_price['WAVAX'][day_json["date"]] = day_json["price"]

    tokens_price["ETH"] = tokens_price["WETH"]
    tokens_price["WMATIC"] = tokens_price["MATIC"]
    tokens_price["USDC.e"] = tokens_price["USDC"]
    tokens_price["WBTC"] = tokens_price["BTC"]
    tokens_price["WBTC.e"] = tokens_price["BTC"]

    # We get all the token decimals
    print(f"Main process - Getting decimals ...")
    latest_blocks = {}
    for pool in pools:
        name0, name1 = pool.pair.split("-")
        names = [name0, name1]
        if pool.chain not in latest_blocks:
            print(f"Querying latest block for chain {pool.chain}")
            latest_block_json = run_query(query_type="get_block_for_date", chain=chain_dict[pool.chain],
                                          start_date=(end_time - timedelta(days=1)).strftime("%Y-%m-%d"),
                                          end_date=end_time.strftime("%Y-%m-%d"))
            blocks = latest_block_json["data"]["items"]
            assert len(blocks) > 0, "Error: No block received when querying the latest block ... "
            latest_blocks[pool.chain] = blocks[-1]['height']

        latest_block = latest_blocks[pool.chain]

        while not (pool.token0 in all_tokens_decimals and pool.token1 in all_tokens_decimals):
            assert latest_block > 0, f"No transaction found for some token in {pool.pair}"
            print(f"{name0} or {name1} on {pool.chain} not in all_tokens_decimals, querying from block {latest_block-800000} to {latest_block}...")
            for ix, address in enumerate([pool.token0, pool.token1]):
                if address not in all_tokens_decimals:

                    result = run_query(query_type="log_events", chain=chain_dict[pool.chain], address=address, page_size=1, starting_block=latest_block-800000, end_block="latest" if latest_block == blocks[-1]['height'] else latest_block)
                    for tx_json in result["data"]["items"]:
                        if tx_json["sender_address"].lower() == address.lower():
                            all_tokens_decimals[address] = tx_json["sender_contract_decimals"]
                            print(f"Main process - Token {names[ix]} on {pool.chain} updated with "
                                  f"{all_tokens_decimals[address]} decimals. Address: {address}")
                            with open(path_to_decimals, mode='w') as f:
                                json.dump(all_tokens_decimals, f, indent=2)
                            break
            latest_block -= 800000


    print(f"Main process - Finished refreshing decimals.")

    current_pool = pools[0]
    current_index = 0
    row = {}
    row["Chain"], row["Pair"], row["Exchange"], row[
        "Address"] = current_pool.chain, current_pool.pair, current_pool.exchange, current_pool.address
    row["ChainExplorer"] = f"{chain_explorers[current_pool.chain + ', ' + current_pool.exchange]}{current_pool.address}"
    name0, name1 = current_pool.pair.split("-")
    reserve0, reserve1 = 0, 0  # Only used for UniswapV3
    daily_rows = {}

    while current_index < len(pools):
        try:
            tx_page = _queue_joiner_to_main.get(block=False)
        except Empty:
            continue

        else:

            while tx_page == "done" or tx_page["data"]["address"] != current_pool.address:
                # Flush existing data
                print(f"Finished querying for pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}. Processing ...")

                # We compute the daily average prices and reserves
                last_reserve0 = {}
                last_reserve1 = {}
                is_new_prices_for_date = {}
                for str_day in daily_rows:
                    for key in daily_rows[str_day]:
                        if isinstance(daily_rows[str_day][key], list):
                            value_list = daily_rows[str_day][key]
                            if key == "Reserve0" and len(value_list) > 0:
                                last_reserve0[str_day] = value_list[-1]
                            elif key == "Reserve1" and len(value_list) > 0:
                                last_reserve1[str_day] = value_list[-1]

                            if len(value_list) == 0:
                                daily_rows[str_day][key] = 0
                            else:
                                if "Price" in key:
                                    is_new_prices_for_date[str_day] = key[-1]
                                daily_rows[str_day][key] = np.mean(daily_rows[str_day][key], dtype=float)

                all_days = [(start_time + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_time - start_time).days)]
                for i, str_day in enumerate(all_str_days):
                    for last_values in [last_reserve0, last_reserve1]:

                        if i == 0:
                            if str_day not in last_values:
                                last_values[str_day] = 0
                            continue

                        previous_day = all_days[i-1]
                        if str_day not in last_values and previous_day in last_values:
                            last_values[str_day] = last_values[previous_day]


                # We update token_prices if not up to date already.
                resave = False
                for str_day in daily_rows:
                    for i, name in enumerate([name0, name1]):
                        other_i = 0 if i == 1 else 1
                        if name not in tokens_price:
                            tokens_price[name] = {}
                        if str_day not in tokens_price[name]:
                            price = daily_rows[str_day]["Price" + str(i)]
                            total_volume_pair = daily_rows[str_day][f"Amount{i}InUsd"] + daily_rows[str_day][f"Amount{i}OutUsd"]
                            total_reserves_in_usd_pair = daily_rows[str_day][f"Reserve{other_i}"] * daily_rows[str_day][f"Price{other_i}"] * 2
                            # If we don't have 100,000$ of volume per day and 100,000$ in reserves, ditch the price as it is imprecise
                            if (total_volume_pair < 10000) or (total_reserves_in_usd_pair < 50000):
                                print(f"Not saving price for day {str_day} for token {name} with Price {price}$ - Total volume {total_volume_pair}$/day - Total reserves {total_reserves_in_usd_pair}$")
                                continue
                            tokens_price[name][str_day] = price
                            print(f"Added price of {name} for day {str_day} in tokens price: {price}")
                            resave = True

                if resave:
                    for ticker in [name0, name1]:
                        print(f"Saving prices of {ticker} ... to {path_to_price}")
                        with open(path_to_price / f"{ticker}.json", mode='w') as f:
                            json.dump(tokens_price[ticker], f, indent=2)

                # We fill blanks if we had values previously
                all_str_days = [(start_time + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_time - start_time).days)]
                for i, str_day in enumerate(all_str_days):
                    if str_day not in daily_rows:
                        if i != 0 and all_str_days[i-1] in daily_rows:
                            daily_rows[str_day] = copy.deepcopy(daily_rows[all_str_days[i-1]])
                            daily_rows[str_day]["Day"] = str_day
                            daily_rows[str_day]["Amount0InUsd"] = 0
                            daily_rows[str_day]["Amount1InUsd"] = 0
                            daily_rows[str_day]["Amount0OutUsd"] = 0
                            daily_rows[str_day]["Amount1OutUsd"] = 0

                            # We take the last of the reserves as reserves
                            daily_rows[str_day]["Reserve0"] = last_reserve0[str_day]
                            daily_rows[str_day]["Reserve1"] = last_reserve1[str_day]

                            if name0 in tokens_price and str_day in tokens_price[name0]:
                                daily_rows[str_day]["Price0"] = tokens_price[name0][str_day]
                            if name1 in tokens_price and str_day in tokens_price[name1]:
                                daily_rows[str_day]["Price1"] = tokens_price[name1][str_day]

                # We process the row before writing it
                processed_daily_rows = copy.deepcopy(daily_rows)
                for str_day in daily_rows:
                    name0, name1 = daily_rows[str_day]["Pair"].split("-")

                    # We add columns with name0 and name1
                    processed_daily_rows[str_day]["Name0"] = name0
                    processed_daily_rows[str_day]["Name1"] = name1

                    # We always want ohm as the first asset in the pair.
                    if name1 in possible_assets:
                        processed_daily_rows[str_day]["Name0"] = name1
                        processed_daily_rows[str_day]["Name1"] = name0
                        processed_daily_rows[str_day]["Pair"] = name1 + "-" + name0
                        processed_daily_rows[str_day]["Price0"] = daily_rows[str_day]["Price1"]
                        processed_daily_rows[str_day]["Price1"] = daily_rows[str_day]["Price0"]
                        processed_daily_rows[str_day]["Reserve0"] = float(daily_rows[str_day]["Reserve1"])
                        processed_daily_rows[str_day]["Reserve1"] = float(daily_rows[str_day]["Reserve0"])
                        processed_daily_rows[str_day]["Amount0InUsd"] = float(daily_rows[str_day]["Amount1InUsd"])
                        processed_daily_rows[str_day]["Amount1InUsd"] = float(daily_rows[str_day]["Amount0InUsd"])
                        processed_daily_rows[str_day]["Amount0OutUsd"] = float(daily_rows[str_day]["Amount1OutUsd"])
                        processed_daily_rows[str_day]["Amount1OutUsd"] = float(daily_rows[str_day]["Amount0OutUsd"])

                    # We compute the volume
                    processed_daily_rows[str_day]["Volume"] = np.mean([
                        processed_daily_rows[str_day]["Amount0InUsd"] + processed_daily_rows[str_day]["Amount1InUsd"],
                        processed_daily_rows[str_day]["Amount0OutUsd"] + processed_daily_rows[str_day]["Amount1OutUsd"]
                    ])

                    # We compute the ohm flows
                    processed_daily_rows[str_day]["Flow"] = processed_daily_rows[str_day]["Amount0OutUsd"] - \
                                                            processed_daily_rows[str_day]["Amount0InUsd"]

                    # We compute the usd reserves
                    processed_daily_rows[str_day]["Reserve0Usd"] = processed_daily_rows[str_day]["Reserve0"] * processed_daily_rows[str_day]["Price0"]
                    processed_daily_rows[str_day]["Reserve1Usd"] = processed_daily_rows[str_day]["Reserve1"] * processed_daily_rows[str_day]["Price1"]

                    # We compute the total reserves
                    processed_daily_rows[str_day]["Total reserves"] = processed_daily_rows[str_day]["Reserve0Usd"] + \
                                                                      processed_daily_rows[str_day]["Reserve1Usd"]

                # We write to file
                write_names = (not path_to_liquidity_csv.exists())

                print(f"Finished processing for pair {current_pool.pair} on chain {current_pool.chain} at "
                      f"address {current_pool.address}. Writing to {path_to_liquidity_csv} ...")
                # Create csv file and write column names
                if len(processed_daily_rows) > 0:
                    column_names = list(processed_daily_rows[list(processed_daily_rows)[0]].keys())
                    with open(path_to_liquidity_csv, mode='a', newline='') as f:
                        writer = None
                        for i, str_day in enumerate(all_str_days):
                            if i == 0:
                                writer = csv.DictWriter(f, fieldnames=column_names,
                                                        delimiter=";")
                                if write_names:
                                    writer.writeheader()
                            if str_day in processed_daily_rows:
                                writer.writerow(processed_daily_rows[str_day])

                print(f"Finished writing for pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}")
                current_index += 1
                if current_index < len(pools) and tx_page != "done":
                    current_pool = pools[current_index]
                    # Reinitialize everything.
                    row = {}
                    row["Chain"], row["Pair"], row["Exchange"], row["Address"] = current_pool.chain, current_pool.pair, current_pool.exchange, current_pool.address
                    row["ChainExplorer"] = f"{chain_explorers[current_pool.chain + ', ' + current_pool.exchange]}{current_pool.address}"
                    name0, name1 = current_pool.pair.split("-")
                    reserve0, reserve1 = 0, 0  # Only used for UniswapV3
                    daily_rows = {}
                    print(f"Switching to pair {current_pool.pair} on chain {current_pool.chain} at address {current_pool.address}")
                else:
                    print(f"Done.")
                    return

            # We're getting transactions regarding current_pool and gather them
            for tx_json in tx_page["data"]["items"]:
                tx_date = parser.parse(tx_json["block_signed_at"])
                if tx_date > end_time:
                    continue
                elif tx_date < start_time and False: #current_pool.exchange != "UniswapV3":
                    continue
                elif not tx_json["successful"]:
                    continue
                else:
                    should_update = not(tx_date < start_time)
                    # If first tx of this day, initialize daily rows
                    str_day = tx_date.strftime("%Y-%m-%d")
                    if str_day not in daily_rows and should_update:
                        print(f"Starting processing transactions from {str_day} for {current_pool.pair} on {current_pool.chain} - "
                              f"{current_pool.address}")
                        daily_rows[str_day] = copy.deepcopy(row)
                        # We initialize as lists because we'll average over the whole day.
                        daily_rows[str_day]["Day"] = str_day
                        daily_rows[str_day]["Price0"] = tokens_price[name0][str_day] if name0 in tokens_price and str_day in tokens_price[name0] else []
                        daily_rows[str_day]["Price1"] = tokens_price[name1][str_day] if name1 in tokens_price and str_day in tokens_price[name1] else []
                        daily_rows[str_day]["Reserve0"], daily_rows[str_day]["Reserve1"] = [], []
                        daily_rows[str_day]["Amount0InUsd"], daily_rows[str_day]["Amount0OutUsd"] = 0, 0
                        daily_rows[str_day]["Amount1InUsd"], daily_rows[str_day]["Amount1OutUsd"] = 0, 0

                    # We look through the events to catch a "Sync" event indicating reserve rebalance and "Swap" events
                    # indicating a swap took place.
                    for log_events_json in tx_json["log_events"][::-1]:
                        if log_events_json["sender_address"] == current_pool.address:
                            if log_events_json["decoded"] is None:
                                print(f"Detected a None event in tx {tx_json}")
                                continue
                            elif log_events_json["decoded"]["name"] in ["Mint", "Burn"]:

                                # Update reserves
                                event_json = log_events_json["decoded"]
                                event_names = [event_json["params"][i]["name"] for i in
                                                   range(len(event_json["params"]))]
                                sign = int(log_events_json["decoded"]["name"] == "Mint") * 2 - 1
                                if current_pool.exchange == "UniswapV3":
                                    reserve0 += float(event_json["params"][event_names.index("amount0")]["value"]) / 10 ** all_tokens_decimals[current_pool.token0] * sign
                                    reserve1 += float(event_json["params"][event_names.index("amount1")]["value"]) / 10 ** all_tokens_decimals[current_pool.token1] * sign
                                    # print(f"Updated UniswapV3 reserves for {str_day} for {current_pool.pair} on {current_pool.chain} - "
                                    #       f"{current_pool.address}")
                                else:
                                    reserve0 += float(
                                        event_json["params"][event_names.index("amount0")]["value"]) / 10 ** \
                                                all_tokens_decimals[current_pool.token0] * sign
                                    reserve1 += float(
                                        event_json["params"][event_names.index("amount1")]["value"]) / 10 ** \
                                                all_tokens_decimals[current_pool.token1] * sign
                                    if str_day in daily_rows:
                                        daily_rows[str_day]["Reserve0"].append(reserve0)
                                        daily_rows[str_day]["Reserve1"].append(reserve1)
                                    # print(f"Updated UniswapV2 reserves for {str_day} for {current_pool.pair} on "
                                    #       f"{current_pool.chain} - {current_pool.address}- Reserve0: {reserve0}"
                                    #       f"Reserve1: {reserve1}")

                            elif log_events_json["decoded"]["name"] == "Swap" and current_pool.exchange == "UniswapV3":
                                swap_event_json = log_events_json["decoded"]
                                swap_names = [swap_event_json["params"][i]["name"] for i in
                                              range(len(swap_event_json["params"]))]

                                amount0 = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount0")]["value"]) / \
                                          np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token0]))
                                amount1 = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount1")]["value"]) / \
                                          np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token1]))

                                reserve0 += amount0
                                reserve1 += amount1

                                if should_update:

                                    if amount0 >= 0:
                                        amount0Out = amount0
                                        amount0In = 0
                                    else:
                                        amount0In = -amount0
                                        amount0Out = 0

                                    if amount1 >= 0:
                                        amount1Out = amount1
                                        amount1In = 0
                                    else:
                                        amount1In = -amount1
                                        amount1Out = 0

                                    # We get the price from the transaction and the new reserves.
                                    # https://docs.uniswap.org/sdk/guides/fetching-prices
                                    # https://uniswap.org/whitepaper-v3.pdf
                                    sqrtRatioX96 = (np.longdouble(
                                        swap_event_json["params"][swap_names.index("sqrtPriceX96")][
                                            "value"]) ** 2) / np.longdouble(2 ** 192) * 10 ** (
                                                           all_tokens_decimals[current_pool.token0] - all_tokens_decimals[current_pool.token1])

                                    if isinstance(daily_rows[str_day]["Price0"], list):
                                        #assert name1 in tokens_price and str_day in tokens_price[name1], f"Can't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?"
                                        if name1 in tokens_price and str_day in tokens_price[name1]:
                                            price1 = daily_rows[str_day]["Price1"]
                                            price0 = sqrtRatioX96 * price1
                                            daily_rows[str_day]["Price0"].append(price0)
                                            # print(f"Inferred value of {name0} on {tx_date} to be {price0} usd.")
                                        else:
                                            price0 = price1 = 0
                                            print(f"Can't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?")
                                    elif isinstance(daily_rows[str_day]["Price1"], list):
                                        #assert name0 in tokens_price and str_day in tokens_price[name0], f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?"
                                        if name0 in tokens_price and str_day in tokens_price[name0]:
                                            price0 = daily_rows[str_day]["Price0"]
                                            price1 = 1 / sqrtRatioX96 * price0
                                            daily_rows[str_day]["Price1"].append(price1)
                                            # print(f"Inferred value of {name1} on {tx_date} to be {price1} usd.")
                                        else:
                                            price0 = price1 = 0
                                            print(f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?")
                                    else:
                                        price0 = daily_rows[str_day]["Price0"]
                                        price1 = daily_rows[str_day]["Price1"]

                                    daily_rows[str_day]["Amount0InUsd"] += amount0In * price0
                                    daily_rows[str_day]["Amount0OutUsd"] += amount0Out * price0
                                    daily_rows[str_day]["Amount1InUsd"] += amount1In * price1
                                    daily_rows[str_day]["Amount1OutUsd"] += amount1Out * price1
                                    daily_rows[str_day]["Reserve0"].append(reserve0)
                                    daily_rows[str_day]["Reserve1"].append(reserve1)

                            elif log_events_json["decoded"]["name"] == "Sync" and should_update:
                                assert current_pool.exchange != "UniswapV3"
                                skipped = False
                                rebalance_event_json = log_events_json["decoded"]

                                # We get the reserves and current price from reserves if not already in token_prices
                                rebalance_names = [rebalance_event_json["params"][i]["name"] for i in
                                                   range(len(rebalance_event_json["params"]))]
                                reserve0 = np.longdouble(
                                    rebalance_event_json["params"][rebalance_names.index("reserve0")]["value"]) / \
                                           np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token0]))
                                reserve1 = np.longdouble(
                                    rebalance_event_json["params"][rebalance_names.index("reserve1")]["value"]) / \
                                           np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token1]))

                                if isinstance(daily_rows[str_day]["Price0"], list):
                                    # assert name1 in tokens_price and str_day in tokens_price[name1], f"Can't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?"
                                    if name1 in tokens_price and str_day in tokens_price[name1]:
                                        price1 = daily_rows[str_day]["Price1"]
                                        price0 = reserve1 * price1 / reserve0
                                        daily_rows[str_day]["Price0"].append(price0)
                                    else:
                                        price0 = price1 = 0
                                        print(f"Couldn't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?")
                                elif isinstance(daily_rows[str_day]["Price1"], list):
                                    # assert name0 in tokens_price and str_day in tokens_price[name0], f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?"
                                    if name0 in tokens_price and str_day in tokens_price[name0]:
                                        price0 = daily_rows[str_day]["Price0"]
                                        price1 = reserve0 * price0 / reserve1
                                        daily_rows[str_day]["Price1"].append(price1)
                                    else:
                                        price0 = price1 = 0
                                        print(f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?")
                                else:
                                    price0 = daily_rows[str_day]["Price0"] if daily_rows[str_day]["Price0"] is not None else 0
                                    price1 = daily_rows[str_day]["Price1"] if daily_rows[str_day]["Price1"] is not None else 0


                                daily_rows[str_day]["Reserve0"].append(reserve0)
                                daily_rows[str_day]["Reserve1"].append(reserve1)

                            elif log_events_json["decoded"]["name"] == "Swap" and should_update:
                                swap_event_json = log_events_json["decoded"]

                                # We get the volume from the swap
                                swap_names = [swap_event_json["params"][i]["name"] for i in
                                              range(len(swap_event_json["params"]))]

                                assert "amount0In" in swap_names  # UniswapV2

                                amount0In = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount0In")]["value"]) / \
                                            np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token0]))
                                amount0Out = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount0Out")]["value"]) / \
                                             np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token0]))
                                amount1In = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount1In")]["value"]) / \
                                            np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token1]))
                                amount1Out = np.longdouble(
                                    swap_event_json["params"][swap_names.index("amount1Out")]["value"]) / \
                                             np.longdouble(np.float_power(10, all_tokens_decimals[current_pool.token1]))

                                # print(
                                #     f"Transaction {tx_json['tx_hash']} - Chain {current_pool.chain} - Pair {current_pool.pair} - Uniswap{version} - "
                                #     f"amount0inusd {amount0In * price0} - amount1inusd {amount1In * price1} - "
                                #     f"amount0outusd {amount0Out * price0} - amount1outusd {amount1Out * price1}")

                                if isinstance(daily_rows[str_day]["Price0"], list):
                                    # assert name1 in tokens_price and str_day in tokens_price[name1], f"Can't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?"
                                    if name1 in tokens_price and str_day in tokens_price[name1]:
                                        price1 = daily_rows[str_day]["Price1"]
                                        price0 = (amount1In + amount1Out) / (amount0In + amount0Out) * price1
                                        daily_rows[str_day]["Price0"].append(price0)
                                        # print(f"Inferred value of {name0} on {tx_date} to be {price0} usd.")
                                    else:
                                        price0 = price1 = 0
                                        print(f"Couldn't infer value of {name0} without the value of {name1} on the {str_day}. Try changing the order of the pools ?")
                                elif isinstance(daily_rows[str_day]["Price1"], list):
                                    # assert name0 in tokens_price and str_day in tokens_price[name0], f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?"
                                    if name0 in tokens_price and str_day in tokens_price[name0]:
                                        price0 = daily_rows[str_day]["Price0"]
                                        price1 = (amount0In + amount0Out) / (amount1In + amount1Out) * price0
                                        daily_rows[str_day]["Price1"].append(price1)
                                        # print(f"Inferred value of {name1} on {tx_date} to be {price1} usd.")
                                    else:
                                        price0 = price1 = 0
                                        print(f"Can't infer value of {name1} without the value of {name0} on the {str_day}. Try changing the order of the pools ?")
                                else:
                                    price0 = daily_rows[str_day]["Price0"] if daily_rows[str_day]["Price0"] is not None else 0
                                    price1 = daily_rows[str_day]["Price1"] if daily_rows[str_day]["Price1"] is not None else 0

                                daily_rows[str_day]["Amount0InUsd"] += amount0In * price0
                                daily_rows[str_day]["Amount0OutUsd"] += amount0Out * price0
                                daily_rows[str_day]["Amount1InUsd"] += amount1In * price1
                                daily_rows[str_day]["Amount1OutUsd"] += amount1Out * price1
                            elif "reward" in log_events_json["decoded"]["name"].lower() or "harvest" in log_events_json["decoded"]["name"].lower() and should_update:
                                print("Break here")


if __name__ == "__main__":
    queues_queriers_to_joiner = [Queue() for _ in range(nb_processes)]
    queue_joiner_to_main = Queue()

    queriersProcesses = [Process(target=querier, args=(idx, nb_processes, queues_queriers_to_joiner[idx])) for idx in range(nb_processes)]
    joinerProcess = Process(target=joiner, args=(queues_queriers_to_joiner, queue_joiner_to_main))

     # We launch the processes
    for process in queriersProcesses:
        process.start()
        time.sleep(1)

    joinerProcess.start()

    main(queue_joiner_to_main)

    # We get the number of holders and TVL if needed
    path_to_holders = SAVE_TO_FOLDER / f"holders_{end_time.strftime('%Y-%m-%d')}.csv"
    all_str_days = [(start_time + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_time - start_time).days)]
    path_to_price = SAVE_TO_FOLDER / "prices"
    tokens_price = {}

    if path_to_price.exists():
        for price_json_path in path_to_price.iterdir():
            if ".json" in price_json_path.name:
                ticker = ".".join(price_json_path.name.split(".")[:-1])
                print(f"Loading prices from {price_json_path} ...")
                with open(price_json_path, "r") as f:
                    loaded_token_price = json.load(f)

                if ticker not in tokens_price:
                    tokens_price[ticker] = {}
                for str_day in loaded_token_price:
                    if str_day not in tokens_price[ticker]:
                        print(f"Loaded price {loaded_token_price[str_day]} for {ticker} on {str_day}")
                        tokens_price[ticker][str_day] = loaded_token_price[str_day]

        print("Finished loading prices.")

    if not path_to_holders.exists():
        chain_holders_sets = {asset.split(" - ")[1]: {date: set() for date in all_str_days} for asset in asset_addresses}
        chain_tvls = {asset.split(" - ")[1]: {date: 0 for date in all_str_days} for asset in asset_addresses}

        for asset in asset_addresses:
            asset_address = asset_addresses[asset]
            asset_name, chain_name = asset.split(" - ")
            # for i in range((end_time - start_time).days):
            start_dates = [start_time + timedelta(days=j) for j in range(nb_processes) if start_time + timedelta(days=j) < end_time]

            end_dates = [start_date + timedelta(days=1) for start_date in start_dates]
            start_dates_str = [start_date.strftime("%Y-%m-%d") for start_date in start_dates]
            end_dates_str = [end_date.strftime("%Y-%m-%d") for end_date in end_dates]
            pages = [0 for j in range(nb_processes)]
            nb_processes_ended = 0
            blocks_at_date = [
                run_query("get_block_for_date", chain=chain_dict[chain_name], start_date=start_dates_str[j],
                          end_date=end_dates_str[j])["data"]["items"][0]["height"] for j in range(len(start_dates))]

            while len(start_dates) != 0:
                [print(f"Querying page {pages[j]} for holders of {asset} on {start_dates[j]} - block {blocks_at_date[j]} ...") for j in range(len(start_dates))]
                queries = [COVALENT_ENDPOINT + f"{chain_dict[chain_name]}/tokens/{asset_address}/token_holders/?key={API_KEY}&block-height={blocks_at_date[j]}&page-size=10000&page-number={pages[j]}" for j in range(len(start_dates))]
                token_holders_results = asyncio.run(get_asynch_urls(queries))

                to_remove = []
                for j, token_holders_result in enumerate(token_holders_results):
                    nb_holders_on_this_page = len(token_holders_result["data"]["items"])
                    chain_holders_sets[chain_name][start_dates_str[j]] = chain_holders_sets[chain_name][start_dates_str[j]].union([_json["address"] for _json in token_holders_result["data"]["items"]])
                    if nb_holders_on_this_page > 0:
                        if asset_name in ["sOHMv1", "sOHM", "sSPA", "sHEC", "sHECv1"]:
                            price = 0
                        else:
                            if start_dates_str[j] in tokens_price[asset_name]:
                                price = tokens_price[asset_name][start_dates_str[j]]
                            else:
                                print(f"Didn't find a price for {asset_name} on {start_dates_str[j]}"
                                      f" - Setting price to 0 for TVL computation.")
                                price = 0

                        chain_tvls[chain_name][start_dates_str[j]] += sum([float(token_holders_result["data"]["items"][x]["balance"]) for x in range(len(token_holders_result["data"]["items"])) if token_holders_result["data"]["items"][x]["address"] != "0x0000000000000000000000000000000000000000"]) / 10**token_holders_result["data"]["items"][0]["contract_decimals"] * price
                    should_continue = (token_holders_result["data"]["pagination"] is not None) and\
                                      (token_holders_result["data"]["pagination"]["has_more"] != False) and \
                                      (nb_holders_on_this_page == 10000)
                    if should_continue:
                        pages[j] += 1
                    else:
                        pages[j] = 0
                        start_dates[j] += timedelta(days=nb_processes)
                        end_dates[j] += timedelta(days=nb_processes)
                        start_dates_str[j] = start_dates[j].strftime("%Y-%m-%d")
                        end_dates_str[j] = end_dates[j].strftime("%Y-%m-%d")
                        if end_dates[j] > end_time:
                            to_remove.append(j)
                        else:
                            blocks_at_date[j] = \
                            run_query("get_block_for_date", chain=chain_dict[chain_name], start_date=start_dates_str[j],
                                      end_date=end_dates_str[j])["data"]["items"][0]["height"]

                for j in to_remove[::-1]:
                    nb_processes_ended += 1
                    print(f"Process {nb_processes_ended}/{nb_processes} finished for {asset}")
                    del start_dates[j]
                    del end_dates[j]
                    del start_dates_str[j]
                    del end_dates_str[j]
                    del pages[j]
                    del blocks_at_date[j]

        with open(path_to_holders, mode='a', newline='') as f:
            column_names = ["chain", "date", "nb_holders", "TVL usd"]
            writer = csv.DictWriter(f, fieldnames=column_names,
                                             delimiter=";")
            writer.writeheader()
            for chain in chain_holders_sets:
                for str_day in chain_holders_sets[chain]:
                    writer.writerow({"chain": chain, "date": str_day, "nb_holders": len(chain_holders_sets[chain][str_day]), "TVL usd": chain_tvls[chain][str_day]})

        del chain_holders_sets
