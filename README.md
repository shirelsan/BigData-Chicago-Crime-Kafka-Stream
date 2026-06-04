# Chicago Crime Data Streaming Analytics (Lab 1)

**Team Members:**
* Shirel Bodenheimer (322328824)
* Tehila Ben-David (314692195)

**Submission Date:** 2026-06-04
**Git Repository:** [BigData-Chicago-Crime-Kafka-Stream](https://github.com/YOUR_USERNAME/BigData-Chicago-Crime-Kafka-Stream)

## 1. Environment Setup

**Run Zookeeper:**
```cmd
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

**Run Kafka Broker:**
```cmd
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Create Kafka Topic:**
```cmd
cd C:\kafka\bin\windows
kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic phonecall-stream
```

## 2. Run the Application

Note: The consumer must be started before the producer to capture all streamed data and recreate the correct delays.

**Step 1: Start the Consumer**
```cmd
cd C:\BigData\lab1
python consume_stream.py > consumer.console.txt 2>&1
```

**Step 2: Start the Producer (Time window: w=7 days)**
```cmd
cd C:\BigData\lab1
python produce_stream.py > producer.console.txt 2>&1
```

## 3. Results Snapshot (w=7 days)

Example of Output snippet extracted from consumer.console.txt all data is present in the file:
```cmd
time,district,TopLocationDescription,MeanTime
2002-07-04 03:00:00,20,STREET;3,14.735825050184886
2002-07-04 03:00:00,21,STREET;7 ; PARKING LOT/GARAGE(NON.RESID.);1,12.194197096978643
2002-07-04 03:00:00,22,STREET;9 ; OTHER;1,11.195403700170313
2002-07-04 03:00:00,23,STREET;9 ; PARK PROPERTY;1,13.769174849441594
2002-07-04 03:00:00,24,STREET;10 ; OTHER;1,13.291523190768897
2002-07-04 03:00:00,25,STREET;32 ; GAS STATION;1,11.942850796603754
...
```

