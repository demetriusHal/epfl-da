package cs451;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class PerfectLink {

	static boolean debug = false;
	
    public final int port;


    //metadata
    //currently this is the same for all messages, so this can easily go out of proportion
    HashSet<Message> delivered;
    
    //uses
    DatagramManager manager;

    //delivery Method
    Delivery deliverPL;
    Delivery deliverAbove;

    PerfectLink(int port, List<Host> hlist, Delivery deliverAbove) {
    	this.deliverAbove = deliverAbove;
        this.port = port;
        delivered = new HashSet<Message>();

        deliverPL = new Delivery() {
            public void deliver(Message m, int from) {
                PerfectLink.this.deliver(m, from);
            }
        };
        manager = new DatagramManager(port, hlist, deliverPL);


    }

    void send(Message m, int destination) {
    	//System.out.printf("PerfectLink> Sending %d %d to %d\n",m.sequenceNum, m.sender, destination);
        manager.send(m, destination);
    }


    //this will be called with call-back style
    //to match 
    void deliver(Message m, int source) {
        if (delivered.contains(m))
            return ;
        
        synchronized(this) {
        	delivered.add(m.clone());
        }
        if (PerfectLink.debug == true)
        	System.out.println("\t\tPerfectLink> Delivered "+ m.sequenceNum+" "+m.sender+" " +m.from);
        deliverAbove.deliver(m, source);
    }


    public void finalize() {
    	manager.finalize();
    }

   
}