
class Consumer {
	string ip;
	string port;
	String groupId;
	vector<Broker> brokers;
	List<Pair<int, Broker>> subscribedPartitions;
	List<Consumer> group;

public:
	Consumer(String groupId, String topic){
		this.groupId = groupId;
		this.topic = topic;
		subscribre();
	}

	void subscribre(string topic) {
		// ask defaut broker this group's coordinator (broker)
		Broker defaultBroker = pickBroker();
		Broker coordinator = findCoordinator(broker);
		subscribedPartitions = findSubscribedPartition(coordinator);
	}

	vector<ConsumerRecord> poll() {
		
		// multicast of each partition in subscribedPartitions;
		while (true) {
			// new Thread接收回传讯息
		}		
	}

	// to coordinator
	void sendHeartBeat() {

	}

	Map<String, List<Pair<partition, Broker>>> rebalance(String groupId) {

	}
};