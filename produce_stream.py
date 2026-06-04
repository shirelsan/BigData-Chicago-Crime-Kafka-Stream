import pandas as pd
from kafka import KafkaProducer
import json
import time
import numpy as np

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'phonecall-stream'
#CSV_PATH = r'C:\BigData\lab1\mvtWeek1.csv'
CSV_PATH = 'mvtWeek1.csv'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=json_serializer
    )

    print(f"Loading data from {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH, low_memory=False)
    
    # 1. Format Dates
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # 2. Impute missing District from Beat (TEST3.2 & TEST4.7)
    df['Beat'] = pd.to_numeric(df['Beat'], errors='coerce')
    df['District'] = pd.to_numeric(df['District'], errors='coerce')
    # First 1 or 2 digits of the Beat code represent the District
    df['District'] = df['District'].fillna(df['Beat'] // 100)
    
    # 3. Clean remaining critical missing info
    df = df.dropna(subset=['Date', 'District', 'LocationDescription'])
    df['District'] = df['District'].astype(int)
    
    # 4. Sort data by Date
    df = df.sort_values(by='Date')
    
    # Generate random ResponseTime
    df['ResponseTime'] = np.random.uniform(5.0, 20.0, size=len(df))

    print(f"Starting to stream data to topic: {TOPIC_NAME} in time-windows...")
    
    # 5. Partition by time-based windows (TEST5.5) - w=7 days
    df.set_index('Date', inplace=True)
    windows = df.groupby(pd.Grouper(freq='7D'))
    
    total_sent = 0
    
    for window_time, group in windows:
        if group.empty:
            continue
            
        print(f"Sending window starting at {window_time} with {len(group)} records...")
        
        for index, row in group.iterrows():
            payload = {
                'Date': index.strftime('%Y-%m-%d %H:%M:%S'),
                'LocationDescription': str(row['LocationDescription']),
                'District': int(row['District']),
                'ResponseTime': float(row['ResponseTime'])
            }
            producer.send(TOPIC_NAME, value=payload)
            total_sent += 1
        
        # Flush the buffer for this specific week and simulate time delay
        producer.flush()
        time.sleep(0.5) 
        
    print(f"Data streaming completed successfully. Total records sent: {total_sent}")

if __name__ == "__main__":
    run_producer()