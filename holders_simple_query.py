from importlib.resources import path

from constants import *
import csv
import numpy as np
from datetime import datetime, timezone, timedelta
from dateutil import parser
import asyncio
from covalent_liquidity_scraper import run_query, get_asynch_urls

# -- 0xmp --
# Minimal script to get OHM-asset holders on different chains.
# Note: Duplicates will be removed for one address holding several assets (eg. both sOHM and gOHM) on one chain,
# but addresses owning OHM on Ethereum and Avalanche will be counted twice. Could be cleaner.

start_time = datetime.now(tz=timezone.utc) - timedelta(days=30)
end_time = start_time + timedelta(days=(datetime.now(tz=timezone.utc) - start_time).days)
nb_processes = 20
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if __name__ == "__main__":

    # We get the number of holders
    path_to_holders = SAVE_TO_FOLDER / f"holders_{end_time.strftime('%Y-%m-%d')}.csv"
    all_str_days = [(start_time + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_time - start_time).days)]

    if not path_to_holders.exists():
        chain_holders_sets = {asset.split(" - ")[1]: {date: set() for date in all_str_days} for asset in asset_addresses}

        for asset in asset_addresses:
            asset_address = asset_addresses[asset]
            asset_name, chain_name = asset.split(" - ")
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
                queries = [COVALENT_ENDPOINT + f"{chain_dict[chain_name]}/tokens/{asset_address}/token_holders/?key={API_KEY}&block-height={blocks_at_date[j]}&page-size=99999&page-number={pages[j]}" for j in range(len(start_dates))]
                token_holders_results = asyncio.run(get_asynch_urls(queries))

                to_remove = []
                for j, token_holders_result in enumerate(token_holders_results):
                    nb_holders_on_this_page = len(token_holders_result["data"]["items"])
                    chain_holders_sets[chain_name][start_dates_str[j]] = chain_holders_sets[chain_name][start_dates_str[j]].union([_json["address"] for _json in token_holders_result["data"]["items"]])
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

        print(f"Writing file to {path_to_holders}.")
        with open(path_to_holders, mode='a', newline='') as f:
            column_names = ["chain", "date", "nb_holders"]
            writer = csv.DictWriter(f, fieldnames=column_names, delimiter=";")
            writer.writeheader()
            for chain in chain_holders_sets:
                for str_day in chain_holders_sets[chain]:
                    writer.writerow({"chain": chain, "date": str_day, "nb_holders": len(chain_holders_sets[chain][str_day])})

        del chain_holders_sets

    print("Done.")
