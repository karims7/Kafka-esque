
/**
 * the broker package containes the server side itself
 * 
 * broker is one single post office
 * in real kafka there are multiple there are multiple post offices (brokers) running at the same time across different computers. 
 * so the system needs a way to keep track of each one. 
 * Thats what brokerinfo is for: its a name tag for a broker.
 * 
 * 
 * 
 * 
 * 
 * id: unique number assigned to each broker (post office): broker 1, broker 2, broker 3 etc. 
 * when the system needs to say "send this message to broker 2", it uses this number. 
 * 
 * 
 * host: the address of the computer the broker (post office) is running on. for example, "192.168.1.5"
 * 
 * 
 * port: the "specific" door on that computer. a computer can run many programs at once. and each program listens on a different port number.
 * like apartment numbers in a building. the apartment is port. For example, port 9092 is what real kafka uses by default. 
 * 
 * 
 * equals(): method every java object has. BrokerInfo objects will have this method.
 * the system will compare brokers constantly — "is this broker already in my list?" or "is this broker the leader for this partition?" 
 * We want Java to answer those questions by checking the id, not by checking if they're the same object in memory.
 * when the broker list needs to check "do i already know about broker 3?", it can call contains() on a list and Java will use
 * our equals() method to compare by ID correctly. 
 * 
 * Imagine we are 
 * 
 * 
 * hashcode(): number java generated from an object. used as a quick lookup key. 
 * in java there is an unbreakable rule: if two objects are equal according to equals(), they must have the same hashcode.
 * since we changed equals() to compare by id, we must also change hashcode() to be based on id. otherwise, HashMaps and HashSets will break in silent ways. 
 * 
 * 
 * 
 * equals() and hashcode() already exist for every Java object. we dont have to define them.
 * both of these methods are inherited from a class call Object. We change the behaviour because the default versions are basically useless for our purposes.
 * Default equals(): checks if two variables point to the same object in memory - not if they represent the same broker. 
 * Default hashcode(): generates a number based on memory address — not based on the id. 
 * 
 * 
 * 
 * 
 */


package main.java.com.minikafka.broker;

/**
 * Holds information about a broker in the MiniKafka cluster
 */

public class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;


    public BrokerInfo (int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }


    public int getId () {
        return id;
    }

    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }


    @Override
    public boolean equals (Object object) {
        if (this == object) return true; // literally same object in memory so stop here and say they're equal
        if (object == null || getClass() != object.getClass()) return false; // null or not even a BrokerInfo. its not equal

        BrokerInfo other = (BrokerInfo) object; // since it is a broker info, we can cast it to a broker info and check the id
        return id == other.id; // two brokers are same if and only if their id numbers match
    }

    public int hashCode () {
        return Integer.hashCode(id); // hashcode is whatever Java's built in hash is for the id number. 
        // instead of returning a random number based on memory address, we return a number based on the id.
    }















}