class Broker {
	string ip;
	string port;
	Zookeeper zookeeper;

	// 某topic, partition 的其他組員是誰
	unordered_map< string,vector<Broker> > topics_member;
	
	// 作为coordinator要用到的讯息
	unordered_map<string, Broker> topics_coordinator;
	
	// consumer1 -> topic 1, partition0
	//			 -> topic 3, partition2
	// consumer2 -> ..
	Map<Consumer, Map<String, List<Pair<partition, Broker>>>> coordinatorMap;
	// each topic's consumer group leader
	Map<String, Consumer> consumerLeader;
	// 记录consumer， offset
	Map<Consumer, int> consumerOffset;


public:
	Broker(string ip, string port, Zookeeper ) {
		// 對於這個broker 的ip port自己去開一個socket

		// 需要給定zookeeper位址
		// 寄訊息叫zookeeper更新cluster brokers 資料
	}
	// append the message to the corresponding <topic,partition> 
	void accept_producer_msg() {
		// 打開那個file
		// append
		// 傳回成功訊息
	}
	// 收到問topic broker的request
	void handle_producer_query() {
		// 看自己知不知道這個topic 的相關訊息
		// 如果知道
		// 		就回傳
		// 如果不知
		// 		問zookeeper response_topic_partition_leader
		//		回傳
		/* if () {
	
		} else {
	
		}
		
		*/
	}
	// response for consumer.poll(String topic)
	Partition response_poll() {
		// find corresponding partition in map
		// return partition
	}

	// send heart beat to zookeeper
	void send_heartbeat() {

	}

	void handle_consumer_heartbeat() {
		if (异常) {
			// notify leader rebalance
			// update coordinatorMap
		}
	}

	// open the topic partition file, and send record
	ConsumerRecord handle_consumer_querry(int partition, Consumer consumer) {

	}

};