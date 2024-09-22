import os
import pandas as pd
from datetime import datetime, timezone
import json
import argparse
import time
from urllib.parse import urlparse


def transform_data(json_file, unix_timestamp=False):
    with open(json_file, 'r') as file:
        data = []
        for line in file:
            try:
                record = json.loads(line)
                data.append(record)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    rows = []
    for record in data:
        web_browser, operating_sys = None, None
        if 'a' in record and len(record['a'].split()) > 1:
            web_browser = record['a'].split()[0]
            operating_sys = record['a'].split()[1]

        from_url = extract_domain(record.get('r', ''))
        to_url = extract_domain(record.get('u', ''))
        city = record.get('cy', 'Unknown')
        longitude, latitude = record.get('ll', [None, None])
        time_zone = record.get('tz', 'Unknown')

        time_in = convert_timestamp(record['t'], unix_timestamp)
        time_out = convert_timestamp(record['hc'], unix_timestamp)

        rows.append({
            'web_browser': web_browser,
            'operating_sys': operating_sys,
            'from_url': from_url,
            'to_url': to_url,
            'city': city,
            'longitude': longitude,
            'latitude': latitude,
            'time_zone': time_zone,
            'time_in': time_in,
            'time_out': time_out
        })

    df = pd.DataFrame(rows)
    return df

def extract_domain(url):
    return urlparse(url).netloc.split('/')[0]
       
def convert_timestamp(timestamp, unix_timestamp=False):
   
    if timestamp:
        if unix_timestamp:
            return timestamp
        else:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None


def remove_duplicates(df):
    return df.drop_duplicates()

def main(input_path, output_path, unix_timestamp):
    start_time = time.time()

    json_files = [f for f in os.listdir(input_path) if f.endswith('.json')]

    for file in json_files:
        df = transform_data(os.path.join(input_path, file), unix_timestamp)
        df = remove_duplicates(df)

        csv_filename = os.path.splitext(file)[0] + '.csv'
        df.to_csv(os.path.join(output_path, csv_filename), index=False)

        print(f"File {file} transformed with {len(df)} rows and saved to {os.path.join(output_path, csv_filename)}")

    total_time = time.time() - start_time
    print(f"Total execution time: {total_time} seconds")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", type=str, help="Input files path", required=True)
    parser.add_argument("-o", "--output_path", type=str, help="Output files path", required=True)
    parser.add_argument("-u", "--unix_timestamp", action='store_true', help="Use UNIX timestamp")

    args = parser.parse_args()
    input_path = args.input_path
    output_path = args.output_path
    unix_timestamp = args.unix_timestamp

    main(input_path, output_path, unix_timestamp)