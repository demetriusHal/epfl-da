package cs451;

import java.util.List;

public class FIFOBroadcast {

	static boolean debug = false;
	
	
	int port;
	List<Host> hlist;
	URBroadcast urb;
	int id;
	
	Delivery deliverAbove;
	Delivery deliverCallback;
	
	
	FIFOBroadcast(int myid, int port, List<Host> hlist, Delivery deliverAbove) {
		this.deliverAbove = deliverAbove;
		id = myid;
		this.port = port;
		this.hlist = hlist;
		
		deliverCallback = new Delivery() {
			public void deliver(Message m, int from) {
				FIFOBroadcast.this.deliver(m, from);
			}
		};

		this.urb = new URBroadcast(myid , port , hlist, deliverCallback);
	}
	
	
	
	void deliver(Message m, int from) {
		
	}
}
