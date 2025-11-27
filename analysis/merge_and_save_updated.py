import pandas as pd
from pathlib import Path
import shutil
import sys

# Paths
ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = Path(__file__).resolve().parent


def find_date_column(df):
    candidates = ['date', 'datum', 'Datum', 'DATUM']
    for c in df.columns:
        if c.lower() in [x.lower() for x in candidates]:
            return c
    return None


def read_and_normalize(path):
    df = pd.read_csv(path)
    date_col = find_date_column(df)
    if date_col is None:
        raise ValueError(f"No date column found in {path}")
    df[date_col] = pd.to_datetime(df[date_col], dayfirst=False, errors='coerce')
    df = df.rename(columns={date_col: 'date'})
    df = df.rename(columns={c: c.strip() for c in df.columns})
    return df


def find_column(df, keywords):
    kws = [k.lower() for k in keywords]
    for c in df.columns:
        lc = c.lower()
        for k in kws:
            if k in lc:
                return c
    return None


def standardize_sales_df(df):
    # id
    id_col = find_column(df, ['id'])
    if id_col and id_col != 'id':
        df = df.rename(columns={id_col: 'id'})

    # warengruppe
    war_col = find_column(df, ['wareng', 'warengruppe'])
    if war_col and war_col != 'warengruppe':
        df = df.rename(columns={war_col: 'warengruppe'})

    # umsatz (may be missing)
    u_col = find_column(df, ['umsatz'])
    if u_col:
        if u_col != 'umsatz':
            df = df.rename(columns={u_col: 'umsatz'})
        df['umsatz'] = pd.to_numeric(df['umsatz'].astype(str).str.replace(',', '.'), errors='coerce')
    else:
        df['umsatz'] = pd.NA

    # ensure id exists
    if 'id' not in df.columns:
        df['id'] = pd.NA

    df = df.rename(columns={c: c.strip() for c in df.columns})
    return df


def main():
    umsatz_path = ROOT / 'umsatzdaten_gekuerzt.csv'
    wetter_path = ROOT / 'wetter.csv'
    kiwo_path = ROOT / 'kiwo.csv'
    test_path = ANALYSIS_DIR / 'test.csv'

    print('Reading base files...')
    umsatz = read_and_normalize(umsatz_path)
    wetter = read_and_normalize(wetter_path)
    kiwo = read_and_normalize(kiwo_path)

    # prepare sales: optionally append continuation from analysis/test.csv
    umsatz = standardize_sales_df(umsatz)
    if test_path.exists():
        print(f'Appending continuation file: {test_path.name}')
        test_df = read_and_normalize(test_path)
        test_df = standardize_sales_df(test_df)

        desired_cols = ['date', 'id', 'warengruppe', 'umsatz']
        for c in desired_cols:
            if c not in umsatz.columns:
                umsatz[c] = pd.NA
            if c not in test_df.columns:
                test_df[c] = pd.NA

        umsatz.replace('', pd.NA, inplace=True)
        test_df.replace('', pd.NA, inplace=True)

        combined_sales = pd.concat([umsatz[desired_cols], test_df[desired_cols]], ignore_index=True)
        combined_sales['date'] = pd.to_datetime(combined_sales['date'], errors='coerce')
        combined_sales['_id_sort'] = combined_sales['id'].astype(str).fillna('')
        combined_sales = combined_sales.sort_values(by=['date', '_id_sort'], na_position='last').drop(columns=['_id_sort'])
        combined_sales = combined_sales[desired_cols]

        umsatz = combined_sales
        print(f'Combined sales rows: {len(umsatz)}')

    # ensure umsatz numeric
    if 'umsatz' in umsatz.columns:
        umsatz['umsatz'] = pd.to_numeric(umsatz['umsatz'].astype(str).str.replace(',', '.'), errors='coerce')
    else:
        umsatz['umsatz'] = pd.NA

    # Merge (sales as left)
    print('Merging with weather and kiwo (left join on date)...')
    merged = umsatz.merge(wetter, on='date', how='left')
    merged = merged.merge(kiwo, on='date', how='left')

    # Convert selected columns to nullable integer to remove trailing .0 while keeping NA
    int_cols = ['Bewoelkung', 'Windgeschwindigkeit', 'KielerWoche']
    for c in int_cols:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors='coerce').round(0).astype('Int64')

    # Reorder rows: date, warengruppe, id
    sort_keys = [k for k in ['date', 'warengruppe', 'id'] if k in merged.columns]
    if sort_keys:
        merged = merged.sort_values(by=sort_keys, na_position='last')

    # Reorder columns: prefer this order, then keep any remaining columns
    preferred = ['date', 'warengruppe', 'id', 'umsatz', 'Bewoelkung', 'Temperatur', 'Windgeschwindigkeit', 'Wettercode', 'KielerWoche']
    cols_order = [c for c in preferred if c in merged.columns] + [c for c in merged.columns if c not in preferred]
    merged = merged[cols_order]

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYSIS_DIR / 'merged_data_updated.csv'
    # Keep `id` column in the saved merged output again (previously removed).
    # Commented out removal so the `id` stays in the CSV; undo comment if you
    # want to drop `id` in future runs.
    # if 'id' in merged.columns:
    #     merged = merged.drop(columns=['id'])

    # write with visible NaN representation
    merged.to_csv(out_path, index=False, na_rep='NaN')

    print(f'Wrote updated merged CSV to: {out_path} (rows: {len(merged)})')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error during merge:', e, file=sys.stderr)
        raise
