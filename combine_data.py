import pandas as pd

def combine_csvs(csv_list, out_csv='all_data.csv'):
    dfs = [pd.read_csv(f) for f in csv_list]
    all_df = pd.concat(dfs, ignore_index=True)
    all_df = all_df.drop_duplicates(subset='text')
    all_df.to_csv(out_csv, index=False)
    print(f"Combined {len(all_df)} rows to {out_csv}")
    return all_df

if __name__ == '__main__':
    combine_csvs(['tweets.csv', 'pdf_data.csv'])
