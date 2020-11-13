package cs451;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class FIFOBroadcast {

	static boolean debug = false;
	
	
	int port;
	List<Host> hlist;
	URBroadcast urb;
	int id;
	
	Delivery deliverAbove;
	Delivery deliverCallback;
	
	LinkedList<Message>[] messageBuf;
	int[] index;
			
	
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
		
		this.messageBuf = new LinkedList[hlist.size()];
		for (int i=0; i < hlist.size(); i++) {
			messageBuf[i] = new LinkedList<>();
			index[i] = 0;
		}

		this.urb = new URBroadcast(myid , port , hlist, deliverCallback);
	}
	
	
	
	void deliver(Message m, int from) {
		Vector<Message> toBeDelivered = new Vector<>();
		synchronized(this) {
			insertSorted(messageBuf[from], m);
			while (messageBuf[from].size() > 0 && messageBuf[from].get(0).sequenceNum == index[from]) {
				Message deliverable = messageBuf[from].pop();
				index[from] += 1;
				toBeDelivered.add(deliverable);
				
			}
		}
		for (Message dm: toBeDelivered) {
			deliverAbove.deliver(dm);
		}

			
	}
	
	//do binary search here for optimality
	void insertSorted(LinkedList<Message> list, Message m) {
		var it = list.iterator();
		
		int pos = 0;
		while (it.hasNext()) {
			if (it.next().sequenceNum > m.sequenceNum)
				break;
			pos++;
		}
		list.add(pos, m);
		
	}
}
