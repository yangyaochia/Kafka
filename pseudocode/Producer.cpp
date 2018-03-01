class ProducerRecord {
	string topic;
	//int partition;
	//K key;
	V value;
};


class Producer {
	// zookeeper list
	// 起始知道的某些人
	string ip;
	string port;
	unordered_map< Broker> brokers;
	// topic, <partition, 負責的broker>
	unordered_map<string, vector<pair<int,Broker>> > topic_partition_leaders;
public:
	//Construtor
	Producer(vector<Broker> bros) {
		brokers.insert(bros.begin(), bros.end());
	}
	/* create topic 
	void createTopic(string topic, int partition, int replication) {
		//
	}*/	
	// send a message to a specifc topic
	void sendMessage(string topic, string msg) {
		// 第一次傳這個topic的訊息
		if ( topic_partition_leaders.count(topic) == 0 ) {
			// 去問default broker 
		} else {	// 第二次之後直接問負責人

		}
	}
};