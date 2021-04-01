Настройка kafka для windows:
https://dzone.com/articles/running-apache-kafka-on-windows-os

1 ZooKeeper Installation</br>
1.1. Go to your ZooKeeper config directory. For me its C:\zookeeper-3.4.7\conf</br>
1.2. Rename file “zoo_sample.cfg” to “zoo.cfg”</br>
1.3. Open zoo.cfg in any text editor, like Notepad; I prefer Notepad++.</br>
1.4. Find and edit dataDir=/tmp/zookeeper to :\zookeeper-3.4.7\data  </br>
1.5. Add an entry in the System Environment Variables as we did for Java.</br>
  a. Add ZOOKEEPER_HOME = C:\zookeeper-3.4.7 to the System Variables.</br>
  b. Edit the System Variable named “Path” and add ;%ZOOKEEPER_HOME%\bin;</br> 
1.6. You can change the default Zookeeper port in zoo.cfg file (Default port 2181).</br>
1.7. Run ZooKeeper by opening a new cmd and type zkserver.</br>

2.Setting Up Kafka</br>
2.1. Go to your Kafka config directory. For me its C:\kafka_2.11-0.9.0.0\config</br>
2.2. Edit the file “server.properties.”</br>
2.3. Find and edit the line log.dirs=/tmp/kafka-logs” to “log.dir= C:\kafka_2.11-0.9.0.0\kafka-logs.</br>
2.4. If your ZooKeeper is running on some other machine or cluster you can edit “zookeeper.connect:2181” to your custom IP and port. For this demo, we are using the same machine so there's no need to change. Also the Kafka port and broker.id are configurable in this file. Leave other settings as is.</br>
2.5. Your Kafka will run on default port 9092 and connect to ZooKeeper’s default port, 2181.</br>

3. Running a Kafka Server</br>
Important: Please ensure that your ZooKeeper instance is up and running before starting a Kafka server.</br>
3.1. Go to your Kafka installation directory: C:\kafka_2.11-0.9.0.0\</br>
3.2. Open a command prompt here by pressing Shift + right click and choose the “Open command window here” option).</br>
3.3. Now type .\bin\windows\kafka-server-start.bat .\config\server.properties and press Enter.</br>

4. Creating Topics</br>

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo

5.Creating a Producer 

kafka-console-producer.bat --broker-list localhost:9092 --topic test

6.Creating a Consumer

kafka-console-consumer.bat --zookeeper localhost:2181 --topic test

7. Передача сообщения из производителя в тему demo</br>
 {"id":"1","content":"test111"} 

## Build
mvn clean install
