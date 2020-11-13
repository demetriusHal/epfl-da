package cs451;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBroadcast {

	static boolean debug = false;

	// Structure:
	// A URBroadcast message is an  OriginalSender, SequenceNumber
	// Additional data are are added as the message goes in lower levels
	// to simplify abstraction we use the same structure and just zero out fields that dont make sense
	// for higher levels

	int port;
	List<Host> hlist;
	PerfectLink pl;
	int id;

	// per-broadcast data
	 volatile HashSet<Message> delivered;
	 volatile HashSet<Message> pending;
	 volatile HashMap<Message, HashSet<Integer>> ackM;

	// callback after done
	Delivery deliverCallback;
	Delivery deliverAbove;

	URBroadcast(int myid, int port, List<Host> hlist, Delivery deliverAbove) {
		this.deliverAbove = deliverAbove;
		id = myid;
		this.port = port;
		this.hlist = hlist;
		pending = new HashSet<>();
		delivered = new HashSet<>();
		ackM = new HashMap<>();

		deliverCallback = new Delivery() {
			public void deliver(Message m, int from) {
				URBroadcast.this.deliver(m, from);
			}
		};

		this.pl = new PerfectLink(port, hlist, deliverCallback);
	}

	public void broadcast(Message m) {
		synchronized (pending) {
			pending.add(m.clone());
		}
		// BE broadcast
		beBroadcast(m);
	}

	private void beBroadcast(Message m) {

		
		for (int i = 1; i <= hlist.size(); i++) {
//			System.out.println(i);
			if (i != id)
				pl.send(m.clone(), i);
			else if (m.from == id)
				deliver(m.clone(), i);
		}
	}

	void deliver(Message m, final int sender) {
		//simplify message to ignore the the sender
		//hacky fix that allows not use more abstraction for message
		//theoretically, we should require 1 abstraction per algorithm
		m.sender = 0;
		
		synchronized (this) {
			if (ackM.get(m) == null)
				ackM.put(m, new HashSet<>());
			ackM.get(m).add(sender);
		}
		//System.out.printf(">>>>%d BebDelivered %d %d %d - %d\n", id, m.sequenceNum, m.sender, m.from, from);

		if (!pending.contains(m)) {
			synchronized (pending) {
				pending.add(m);

			}
			// we will also send the message
			Message mnew = m.clone();
			mnew.sender = (byte) id;
			beBroadcast(mnew);
		}
		synchronized (this) {
			//System.out.printf("%d > counted %d %d %d %d\n",this.id,m.sequenceNum,m.from, sender,ackM.get(m).size());
			if (ackM.get(m).size() >= hlist.size()/2 && !delivered.contains(m)) {
				
				delivered.add(m);
				
				// todo perhaps add a daemon who checks this here
				if (URBroadcast.debug)
					System.out.printf("\tURBroadcast> Delivered %d by %d - %d\n", m.sequenceNum, m.from, m.sender);
				this.deliverAbove.deliver(m, m.from);
			}
		}
	}
	
    public void finalize() {
    	pl.finalize();
    }
}