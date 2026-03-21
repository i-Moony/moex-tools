import argparse as args
import asyncio
import sys
from datetime import datetime
from pathlib import Path

import aiohttp
import pandas as pd
from lxml import etree

ENGINE_MAP = {
    "акция": "shares",
    "облигация": "bonds",
    "фонд": "etf",
    "фьючерс": "futures",
    "валюта": "currency",
    "индекс": "index",
}

BOARD_MAP = {
    "shares": "TQBR",
    "bonds": "TQCB",
    "etf": "TQTF",
    "futures": "RFUD",
    "currency": "CETS",
    "index": "SNDX",
}

BASE_COLS = ["open", "high", "low", "close", "volume", "value"]

BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/"


def parse_args():
    parser = args.ArgumentParser(
        description="Получение данных дневных свечей с MOEX ISS в удобном для эконометрики формате"
    )

    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default="input.txt",
        help="Файл со списком необходимых бумаг",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default="output.csv",
        help="Файл, в который будут выведены данные",
    )
    parser.add_argument(
        "-f",
        "--fill",
        action="store_true",
        help="Заполнять ли дни без сделок значениями",
    )
    parser.add_argument(
        "-b",
        "--base",
        type=str,
        default="close",
        help="Какой параметр оставить для сравнения",
    )

    parser.add_argument(
        "-s",
        "--stats",
        type=Path,
        default=None,
        help="Файл, в который будет выведена статистика",
    )

    return parser.parse_args()


def get_engine(market: str):
    return ENGINE_MAP[market.lower()]


def get_board(market: str):
    return BOARD_MAP[market.lower()]


def build_moex_url(
    market: str,
    ticker: str,
    date_start: datetime,
    date_end: datetime,
    limit: int,
    start: int,
):
    engine = get_engine(market)
    board = get_board(engine)

    url = (
        BASE_URL
        + engine
        + "/boards/"
        + board
        + "/securities/"
        + ticker
        + "/candles.xml"
        + "?interval=24"
        + "&from="
        + date_start.strftime("%Y-%m-%d")
        + "&till="
        + date_end.strftime("%Y-%m-%d")
    )

    if limit:
        url += "&limit=" + str(limit)

    if start:
        url += "&start=" + str(start)

    return url


async def fetch_page(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        if resp.status != 200:
            raise Exception(f"Ошибка {resp.status} при запросе {url}!")
        content = await resp.text()
        return etree.fromstring(content.encode("utf-8"))


async def get_candles_xml(
    market: str, ticker: str, date_start: datetime, date_end: datetime, limit=100
):
    all_rows = []
    total = None
    start = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = build_moex_url(market, ticker, date_start, date_end, limit, start)
                content = await fetch_page(session, url)
            except Exception as e:
                print(f"Ошибка при загрузке страницы: {e}")
                break

            candles_data = content.xpath("//data[@id='candles']")

            if not candles_data:
                print("В ответе нет свечей!")

            rows = candles_data[0].xpath(".//rows/row")

            if not rows:
                break

            for row in rows:
                row_dict = {attr: row.get(attr) for attr in row.attrib}
                all_rows.append(row_dict)

            if total is None:
                cursor_data = content.xpath("//data[@id='candles.cursor']")
                if cursor_data:
                    cursor_rows = cursor_data[0].xpath(".//rows/row")
                    if cursor_rows:
                        total = int(cursor_rows[0].get("total", 0))

            start += len(rows)

            if len(rows) < limit or (total is not None and start >= total):
                break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    df["begin"] = pd.to_datetime(df["begin"])

    for col in ["open", "high", "low", "close", "volume", "value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def fill_missing_days(
    df: pd.DataFrame, date_start: datetime, date_end: datetime, fill=False
):
    full_range = pd.date_range(start=date_start, end=date_end, freq="D")
    full_df = pd.DataFrame({"begin": full_range})
    merged = pd.merge(full_df, df, on="begin", how="left")
    merged = merged.sort_values("begin").set_index("begin")

    if fill:
        merged = merged.ffill().bfill()

    return merged


def rename_table(df: pd.DataFrame, ticker: str):
    for col in df.columns:
        df[col] = df[col].rename(ticker + col.capitalize())

    return df


def read_input_file(file_path: Path):
    with open(file_path, encoding="utf-8") as file:
        return file.read().splitlines()


def string_to_datetime(date: str):
    date_parts = date.split("-")

    year = int(date_parts[0])
    month = int(date_parts[1])
    day = int(date_parts[2])

    return datetime(year, month, day)


def get_dates(input_file: list[str]):
    dates = input_file[0].split(" ")

    date_start_str = dates[0]
    date_end_str = dates[1]

    date_start = string_to_datetime(date_start_str)
    date_end = string_to_datetime(date_end_str)

    return [date_start, date_end]


def get_securities(input_file: list[str]):
    input_file.pop(0)
    securities = []

    for line in input_file:
        security_data = line.split(" ")
        market = security_data[0].lower()
        ticker = security_data[1].upper()
        securities.append([market, ticker])

    return securities


def merge(all_candles: list[pd.DataFrame], base: str):
    final_df = pd.DataFrame(all_candles[0].index)

    for security in all_candles:
        security = security.filter(regex=f"begin|.*\\_{base}$")

        final_df = pd.merge(final_df, security, on="begin", how="left")

    return final_df.set_index("begin")


def get_stats(final_df: pd.DataFrame):
    return final_df.describe()


async def main():
    args = parse_args()

    input_file_path = args.input
    output_file_path = args.output
    fill = args.fill
    base = args.base.lower()
    output_stats_file_path = args.stats

    if not input_file_path.exists():
        print(f"Файл {input_file_path} должен существовать!")
        sys.exit(1)

    if base not in BASE_COLS:
        print(
            f"Аргумент BASE должен принимать значение из списка {', '.join(BASE_COLS)}"
        )
        sys.exit(2)

    input_file = read_input_file(input_file_path)

    date_start, date_end = get_dates(input_file)
    securities = get_securities(input_file)

    all_candles: list[pd.DataFrame] = []

    for security in securities:
        market, ticker = security

        print(f"Запрос свечей для {market} {ticker}")
        print(
            f"Период {date_start.strftime('%Y-%m-%d')} {date_end.strftime('%Y-%m-%d')}"
        )

        df_candles = await get_candles_xml(market, ticker, date_start, date_end)

        if df_candles.empty:
            print(
                f"Данные по {market} {ticker} не получены. Проверьте тикер, тип и даты."
            )
            continue

        print(f"Получено свечей: {len(df_candles)}")

        df_filled = fill_missing_days(df_candles, date_start, date_end, fill)

        df_filled.columns = [
            f"{ticker}_{i}" if i not in ["begin"] else f"{i}" for i in df_filled.columns
        ]

        print(f"Итоговое количество записей: {len(df_filled)}")
        print()

        all_candles.append(df_filled)

    final_df = merge(all_candles, base)

    print("Итоговая таблица:")
    print(final_df.head())

    final_df.to_csv(output_file_path, encoding="utf-8", decimal=",", sep=";")

    if not output_stats_file_path:
        sys.exit(0)

    print()
    stats = get_stats(final_df)

    print("Математическая статистика:")
    print(stats.head())

    stats.to_csv(output_stats_file_path, encoding="utf-8", decimal=",", sep=";")


if __name__ == "__main__":
    asyncio.run(main())
