import json
import sys
import os
import configparser
import datetime
import subprocess
from dateutil import rrule
import pytz
import csv
import time
from pycaruna import CarunaPlus, TimeSpan


# Variables for date range
str_start_date = '2024-06-01'
end_date_now = True  # Set True if get data to most recent date = day before yesterday
str_end_date = '2024-05-01'  # Define the end date if end_date_now = False

def validate_token():
    config = configparser.ConfigParser()
    config.read('.secrets.txt')

    token_timestamp = config['caruna']['CARUNA_TOKEN_TIMESTAMP']

    if token_timestamp:
        token_time = datetime.datetime.fromisoformat(token_timestamp)
        current_time = datetime.datetime.now()

        if (current_time - token_time).total_seconds() > 3600:
            # Token is older than 1 hour, re-authenticate
            subprocess.run([sys.executable, 'caruna_authenticate.py'])
            config.read('.secrets.txt')  # Reload the updated secrets.txt
            time.sleep(1)

def get_date_range(str_start_date, end_date_now, str_end_date=None):
    local_tz = pytz.timezone('Europe/Helsinki')
    start_date = local_tz.localize(datetime.datetime.strptime(str_start_date, '%Y-%m-%d')).astimezone(pytz.utc)

    finland_time = datetime.datetime.now(pytz.timezone('Europe/Helsinki'))
    if finland_time.hour >= 9:
        today = finland_time - datetime.timedelta(days=1)
    else:
        today = finland_time - datetime.timedelta(days=2)
    today = today.astimezone(pytz.utc)

    if end_date_now:
        end_date = today
    else:
        end_date = local_tz.localize(datetime.datetime.strptime(str_end_date, '%Y-%m-%d')).astimezone(pytz.utc)
        if end_date > today:
            end_date = today

    return list(rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date))

def save_or_validate_and_append(data, filename='consumption2024.csv'):
    columns = [
        "startTime", "invoicedConsumption", "totalConsumption", 
        "production", "soldProduction", "compensatedProduction", 
        "product", "fee", "temperature"
    ]

    def format_value(value):
        return f"{value}".replace('.', ',')

    new_data = []
    if os.path.exists(filename):
        with open(filename, mode='r', newline='') as file:
            reader = csv.DictReader(file, delimiter=';')
            existing_dates = {row['startTime'] for row in reader}
        
        new_data = [
            entry for entry in data 
            if datetime.datetime.fromisoformat(entry['timestamp']).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ') not in existing_dates
        ]
    else:
        new_data = data

    if new_data:
        mode = 'a' if os.path.exists(filename) else 'w'
        with open(filename, mode=mode, newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns, delimiter=';')
            if mode == 'w':
                writer.writeheader()

            for entry in new_data:
                timestamp = datetime.datetime.fromisoformat(entry['timestamp']).astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                product = list(entry['distributionFeeByTransferProductParts'].keys())[0]
                fee = format_value(entry['distributionFeeByTransferProductParts'][product])

                row = {
                    "startTime": timestamp,
                    "invoicedConsumption": format_value(entry.get("invoicedConsumption", 0)),
                    "totalConsumption": format_value(entry.get("totalConsumption", 0)),
                    "production": format_value(entry.get("production", 0)),  # Default to 0 if not present
                    "soldProduction": format_value(entry.get("soldProduction", 0)),  # Default to 0 if not present
                    "compensatedProduction": format_value(entry.get("compensatedProduction", 0)),  # Default to 0 if not present
                    "product": product, 
                    "fee": fee,
                    "temperature": format_value(entry.get("temperature", 0))
                }

                writer.writerow(row)

# Main code block
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('.secrets.txt')

    validate_token()  # Validate the token before proceeding

    token = config['caruna']['CARUNA_TOKEN']
    customer_id = config['caruna']['CARUNA_CUSTOMER_ID']

    if token is None or customer_id is None:
        raise Exception('CARUNA_TOKEN and CARUNA_CUSTOMER_ID must be defined')

    # Get date range
    date_range = get_date_range(str_start_date, end_date_now, str_end_date)

    # Create a Caruna Plus client
    client = CarunaPlus(token)

    # Get customer details and metering points so we can get the required identifiers
    customer = client.get_user_profile(customer_id)
    # print(json.dumps(customer, indent=2))

    contracts = client.get_contracts(customer_id)
    # print(json.dumps(contracts, indent=2))

    # Get metering points, also known as "assets". Each asset has an "assetId" which is needed e.g. to
    # retrieve energy consumption information for a metering point type asset.
    metering_points = client.get_assets(customer_id)
    # print(metering_points)
    # print(json.dumps(metering_points, indent=2))

    asset_id = metering_points[1]['assetId']
    # print(asset_id)

    all_consumption_data = []

    # Loop through each date in the date range
    for date in date_range:
        year = date.year
        month = date.month
        day = date.day

        # Get daily usage for the current date
        consumption_energy = client.get_energy(customer_id, asset_id, TimeSpan.DAILY, year, month, day)
        all_consumption_data.extend(consumption_energy)

    # Validate and append missing data to the CSV file
    save_or_validate_and_append(all_consumption_data)