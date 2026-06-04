# Shirel Bodenheimer 31722145 
# Tehila Ben-David 314692195

run zookeeper:
C:> cd kafka
C:\kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

run kafka:
C:> cd kafka
C:\kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties

create topic:
C:> cd kafka\bin\windows
C:\kafka\bin\windows> kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic phonecall-stream
C:\kafka\bin\windows>

run the scripts:
1.
C:> cd BigData\lab1
C:\BigData\lab1> python consume_stream.py
2.
C:> cd BigData\lab1
C:\BigData\lab1> python produce_stream.py mvtWeek1.csv

Results:
w=7 days
time,district,TopLocationDescription,MeanTime
2007-01-04 02:00:00,7,STREET;40 ; GAS STATION;4,11.987
2012-12-13 02:00:00,10,STREET;28 ; OTHER;4,11.334
2007-11-01 02:00:00,6,STREET;52 ; ALLEY;4,13.601
2009-09-10 03:00:00,8,STREET;28 ; PARKING LOT/GARAGE(NON.RESID.);4,12.387
2004-07-22 03:00:00,13,STREET;28 ; RESIDENCE-GARAGE;4,11.838
2003-12-04 02:00:00,22,STREET;16 ; SCHOOL, PUBLIC, BUILDING;4,12.503
2007-11-08 02:00:00,9,STREET;24 ; PARKING LOT/GARAGE(NON.RESID.);12,12.430
...