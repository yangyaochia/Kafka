class Zookeeper{
	string ip;
	string port;
	// min heap round robin timestamp queue
	priority_queue<pair<time,Broker>, vector<pair<time,Broker>>, greater<pair<time,Broker>>> clusters;
	unordered_map<string, pair<int,Broker>> topic_map;

	public:
	// 接收來自producer的create_topic
	// 回傳這個topic, partition的負責人給傳的那個人
	void update_cluster() {
		// 新建broker
	}
	string register_topic() {
		// socket programming server接資料
	}
	// 某個broker來問的
	vector<pair<int,Broker>> response_topic_partition_leader(string topic) {


		// pair of partition and Broker
		return vector<pair<int,Broker>>();
	}
};