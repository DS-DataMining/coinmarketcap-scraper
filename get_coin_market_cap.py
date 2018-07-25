from coinmarketcap import scrape
import argparse
import concurrent.futures
import datetime
import bs4
import numpy as np
import pandas as pd
import requests
import tqdm
import sys

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--start', type=scrape.str_to_date, default=datetime.date(2013, 4, 28))
        parser.add_argument('--end', type=scrape.str_to_date, default=datetime.date.today())
        parser.add_argument('--symbols', type=str, nargs='*')
        args = parser.parse_args()

        all_df = scrape.parse_all_response(requests.get(scrape.all_url()))

        slugs = all_df.slug.values
        if args.symbols:
            slugs = all_df.loc[all_df.symbol.isin(args.symbols)].slug.values
        symbols = all_df.loc[all_df.slug.isin(slugs)].symbol.values

        urls = [scrape.historical_coin_url(x, args.start, args.end) for x in slugs]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            responses = [x for x in tqdm.tqdm(executor.map(requests.get, urls),
                                              desc='downloading historical coin pages',
                                              total=len(urls))]

        with concurrent.futures.ProcessPoolExecutor() as executor:
            historical_coin_dfs = [x for x in tqdm.tqdm(executor.map(scrape.parse_historical_coin_response, responses),
                                                        desc='parsing historical coin pages',
                                                        total=len(responses)) if x is not None]

        for slug, symbol, historical_coin_df in zip(slugs, symbols, historical_coin_dfs):
            historical_coin_df['slug'] = slug
            historical_coin_df['symbol'] = symbol

        print(pd.concat(historical_coin_dfs).to_csv())
        sys.stdout.flush()

    except Exception as argv:
        print('Arguments parser error' + argv)
    finally:
        pass

if __name__ == '__main__':
    main()