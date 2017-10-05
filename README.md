# spark-hdfs-reader

# Sample Kafka Code

Application has sample code for kafka producer and consumer.

## Getting Started

SimpleProducer - Very basic producer with mandatory and minimum properties.
SynchronousProducer -  Producer sends data to broker with sync mode.(It process next message only after it recieves acknowledgement).
AsynchronousProducer - Producer sends data to broker with Asnyc mode.(Does not wait for ack).


RebalanceListner -  
1) Maintain a list of offsets that are processed and ready to be committed. 
   i.e Consumer need to maintain list of offset instead of relying on the current offset that are maange by kafka
2) Commit the offsets when partitions are going away.




### Prerequisites

Install and Run Zookeeper and Kafka
Create Topic

### Installing


Start Zookeeper:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Start Kafka:
.\bin\windows\kafka-server-start.bat .\config\server.properties


Create topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter-topic


End with an example of getting some data out of the system or using it for a little demo



## Running the tests

HDFSFileReader


argument 0 - path of HDFS file or directory to read
argument 1 - path where file is stored in destination




## Dependencies


	<dependency>
         <groupId>org.scala-lang</groupId>
         <artifactId>scala-library</artifactId>
         <version>${scala.version}</version>
      </dependency>
      <dependency>
         <groupId>junit</groupId>
         <artifactId>junit</artifactId>
         <version>4.4</version>
         <scope>test</scope>
      </dependency>
      <dependency>
         <groupId>org.specs</groupId>
         <artifactId>specs</artifactId>
         <version>1.2.5</version>
         <scope>test</scope>
      </dependency>
      <dependency>
         <groupId>org.apache.spark</groupId>
         <artifactId>spark-streaming_2.11</artifactId>
         <version>2.2.0</version>
      </dependency>
     
      <dependency>
         <groupId>org.scala-lang</groupId>
         <artifactId>scala-xml</artifactId>
         <version>2.11.0-M4</version>
      </dependency>
      <dependency>
         <groupId>org.scala-lang.modules</groupId>
         <artifactId>scala-xml_2.11</artifactId>
         <version>1.0.6</version>
      </dependency>
      
      <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.10</artifactId>
    <version>1.5.2</version>

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management


## Contributing


## Versioning



## Authors

* **Viyaan Jhiingade** - *Initial work* - [Viyaan](https://github.com/Viyaan)



## License



## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc



HDFSFileReader
