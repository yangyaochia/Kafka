# Nano Kafka Briefing
  1. Implement Kafka-based publish-subscribe system 
  2. Producer applications could create specific topics and send messages to that topics anytime and anywhere (decoupled)
  3. Consumer applications could subscrbe to topics and poll the messages anytime and anywhere (decoupled)
  4. Zookeeper in the cluster monitors the "brokers", "topics" and "consumer groups."
  5. Availability is implemented and realised with indirect communication and replication
  6. Parallesim is implemented via partitions in topic and consumers in consumer group.
  7. Handle single point of failure via monitoring the healthiness of each broker. 

# Setup Steps
  1. Run jar file to activate specific node with the corresponding arguments
  
    -> Start Zookeeper Input Format :
        - Identity Zookeeper_host Zookeeper_port
        - e.g. java -jar Kafka.jar Zookeeper localhost 2181
        
    -> Start Broker Input Format :
        - Identity Broker_host Broker_port Zookeeper_host Zookeeper_port
        - e.g. java -jar Kafka.jar Broker localhost 9000 localhost 2181

    -> Start Producer Input Format :
        - Identity Producer_host Producer_port defaultBroker_host defaultBroker_port Topic_Name #Partition #Replication
        - e.g. java -jar Kafka.jar Producer localhost 8000 localhost 9000 DS 3 3

    -> Start Consumer Input Format :
        - Identity Consumer_host Consumer_port Group_Name defaultBroker_host defaultBroker_port Topic_Name1 (Topic_Name2)
        - e.g. java -jar Kafka.jar Consumer localhost 10000 Group1 localhost 9002 DistributedSystem (SCU)
        - At least one topic, at most two topic names

# Sample Running Case
  -> Normal Publish-Subscribe : Test Partition and Consumer Group Functionality
  
    * Setup Example
      1.) Start Zookeeper
      2.) Start 3 Brokers 
      3.) Start 1 Producer and Create a topic1 with 3 partitions and 1 replication
      4.) Start 1 Producer and Create a topic2 with 2 partitions and 1 replication
      5.) Start 1 Consumer in Group1 and Subscribe to topic1
      6.) Start 1 Consumer in Group1 and Subscribe to topic1 and topic2
      
    * Process and Result:
      -> Producer Side:
        1.) Producer send createTopic request to defaultBroker, and defaultBroker forward it to Zookeeper.
        2.) Zookeeper will assign different brokers for each partition in a topic if the topic is not created before.
        3.) Producer will receive the a list of (topic,partition) leaders. Cache these leaders into his defaultBroker.
        4.) Producer will send messages to the corresponding Broker.
      -> Consumer Side:
        5.) Consumer send register/subscribe request to defaultBroker, and defaultBroker forward it to Zookeeper.
        6.) Zookeeper will assign Coordinator(Broker) if this group has not registered before.
        7.) Consumer send poll request to the (topic,partition) leaders, and receive the messages batch by batch.
        (8.)) Consumers in a consumer group will only receive disjoint messgaes from different partition.
      
  2. One broker down : Test Single Point of Failure
  
    * Setup Example
      1.) Start Zookeeper
      2.) Start 3 Brokers 
      3.) Start 1 Producer and Create a topic1 with 1 partition and 3 replications
      4.) Start 1 Consumer in Group1 and Subscribe to topic1
      5.) Shut Down the topic leader and test whether 
        -> Zookeeper could detect the shutdown of a broker
        -> Producer could continue sending messages without human restoration.
        -> Consumer could continue receiving messages without  human restoration.
        
