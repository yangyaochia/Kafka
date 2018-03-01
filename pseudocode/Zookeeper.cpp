class Zookeeper{
	string ip;
	string port;
	unordered_set<Broker> clusters;
	unordered_map<string, pair<int,Broker>> topic_map;
	
	public:

};