import requests 
import boto3
import os 
from dagster import asset, job
import pandas as pd 


url = "https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json"

@asset 
def get_json_data(): 
    result = requests.get(url)
    return result.json()

@asset
def fix_airline_csv():
    json_data = get_json_data()
    df = pd.DataFrame(
            columns=[
                'airport_code',
                'airport_name',
                'time_label',
                'time_month',
                'time_month_name',
                'time_year',
                'stat_delays_carrier',
                'stat_delays_late_aircraft',
                'stat_delays_nas',
                'stat_delays_security',
                'stat_delays_weather',
                'stat_carriers_names',
                'stat_carriers_total',
                'stat_flights_cancelled',
                'stat_flights_delayed',
                'stat_flights_diverted',
                'stat_flights_on_time',
                'stat_flights_total',
                'stat_min_delayed_carrier',
                'stat_min_delayed_late_aircraft',
                'stat_min_delayed_nas',
                'stat_min_delayed_security',
                'stat_min_delayed_total',
                'stat_min_delayed_weather'
        ]
        ) 

    for data in json_data:
        airport_code = data['Airport']['Code']
        airport_name = data['Airport']['Name']
        time_label = data['Time']['Label']
        time_month = data['Time']['Month']
        time_month_name = data['Time']['Month Name']
        time_year = data['Time']['Year']
        delays_statistics = data["Statistics"]["# of Delays"]
        stat_delays_carrier = delays_statistics['Carrier']
        stat_delays_late_aircraft = delays_statistics['Late Aircraft']
        stat_delays_nas = delays_statistics['National Aviation System']
        stat_delays_security = delays_statistics['Security']
        stat_delays_weather = delays_statistics['Weather']
        carriers_statistics = data["Statistics"]['Carriers']
        stat_carriers_names = carriers_statistics['Names']
        stat_carriers_total = carriers_statistics['Total']
        flights_statistics = data["Statistics"]['Flights']
        stat_flights_cancelled = flights_statistics['Cancelled']
        stat_flights_delayed = flights_statistics['Delayed']
        stat_flights_diverted = flights_statistics['Diverted']
        stat_flights_on_time = flights_statistics['On Time']
        stat_flights_total = flights_statistics['Total']
        minutes_delayed_statistics = data["Statistics"]['Minutes Delayed']
        stat_min_delayed_carrier = minutes_delayed_statistics['Carrier']
        stat_min_delayed_late_aircraft = minutes_delayed_statistics['Late Aircraft']
        stat_min_delayed_nas = minutes_delayed_statistics['National Aviation System']
        stat_min_delayed_security = minutes_delayed_statistics['Security']
        stat_min_delayed_total = minutes_delayed_statistics['Total']
        stat_min_delayed_weather = minutes_delayed_statistics['Weather']



        df.loc[df.shape[0]] = [
            airport_code,
            airport_name,
            time_label,
            time_month,
            time_month_name,
            time_year,
            stat_delays_carrier,
            stat_delays_late_aircraft,
            stat_delays_nas,
            stat_delays_security,
            stat_delays_weather,
            stat_carriers_names,
            stat_carriers_total,
            stat_flights_cancelled,
            stat_flights_delayed,
            stat_flights_diverted,
            stat_flights_on_time,
            stat_flights_total,
            stat_min_delayed_carrier,
            stat_min_delayed_late_aircraft,
            stat_min_delayed_nas,
            stat_min_delayed_security,
            stat_min_delayed_total,
            stat_min_delayed_weather   
        ]
        
    return df.to_csv(sep=";",index=False)



@asset
def send_airline_data_to_s3(airline_data):
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3.put_object(
        Bucket='airlines-input-data',
        Key='airlines.csv',
        Body=airline_data
    )

@job
def upload_json_to_s3_pipeline():
    airline_data = fix_airline_csv()
    print(airline_data)
    send_airline_data_to_s3(airline_data)
