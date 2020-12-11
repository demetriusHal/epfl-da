package cs451;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class LCBroadcast {
	static boolean debug = false;
	
	
	int port;
	List<Host> hlist;
	URBroadcast urb;
	int id;
	int lid;
	
	Delivery deliverAbove;
	Delivery deliverCallback;
	
	int[] index;
	
	short[] vectorClock;
	
	//HashSet<Message> pending;

	LinkedList<Message> pending;
	
	int[][] dependencyList;
	
	public ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
	
	LCBroadcast(int myid, int port, List<Host> hlist, Delivery deliverAbove, int[][] depList) {
		this.deliverAbove = deliverAbove;
		id = myid;
		lid = myid-1;
		this.port = port;
		this.hlist = hlist;
		
		deliverCallback = new Delivery() {
			public void deliver(Message m, int from) {
				LCBroadcast.this.deliver(m, from);
			}
		};
		index = new int[hlist.size()];
		for (int i=0; i < hlist.size(); i++) {
			index[i] = 0;
		}

		this.urb = new URBroadcast(myid , port , hlist, deliverCallback);
		
		// init to 0
		vectorClock = new short[Message.vcSize];
		
		//pending = new HashSet<>();
		pending = new LinkedList<>();
		
		dependencyList = depList;
	}
	
	void deliver(Message m, final int sender) {
		short[] snapshot;
		if (sender != id) {
		//{
			synchronized(pending) {
				//pending.add(m)
				pending.addFirst(m);
				//snapshot vectorclock
				//this might need to be atomic to avoid some weird deadlocks
				snapshot = vectorClock.clone();
				//HashSet<Message> setSnapshot = (HashSet<Message>)this.pending.clone();
			}
			synchronized(pending) {
				deliverPending(snapshot, this.pending);
			}
		}
	}
	
	void deliverPending(short[] snapshot, LinkedList<Message>  set) {
		//brute force through the items
		//although the optimal solution would be to maintain a dependency matrix
		//where a number (i,j) would trigger a check on jth message if the ith process got an update
		boolean converged = false;
		HashSet<Message> delivered = new HashSet<>();
		
		//1 big lock delivery , could be problematic, especially when this could reasonable take a very long time 
		//to improve this, add a dependacy graph here
		while (!converged) {
			converged = true;
			for (Message m: set) {
				if (compareClocks(snapshot, m.vectorClock, m.from)) {
					this.deliverAbove.deliver(m, m.from);
					delivered.add(m);
					set.remove(m);
					synchronized(vectorClock) {
						vectorClock[m.from-1] += 1;
						snapshot[m.from-1] += 1;
					}
					converged = false;
					break;
				}
			}
		}
		//System.out.printf("Remaining: %d Messages\n", set.size());
 	}
	
	public void broadcast(Message m) {
		//deliver to self
		this.deliverAbove.deliver(m, id);
		m.setVC(vectorClock.clone());
		urb.broadcast(m);
		
		synchronized(vectorClock) {
			vectorClock[lid] += 1;
		}
		
	}
	
	public void threadBroadcast(Message m) {
		threadBroadcaster t = new threadBroadcaster(m);
		executor.execute(t);
	}
	

	
	boolean compareClocks(short[] snapshot,short[] sender, int from) {
		from = from-1;
		
		if (snapshot[from] < sender[from])
			return false;

		for (int i=0; i < dependencyList[from].length && dependencyList[from][i] != -1 ; i++) {
			int index = dependencyList[from][i];
			if (snapshot[index] < sender[index])
				return false;
		}
		return true;
	}
	
	
	public class threadBroadcaster extends Thread {
		Message m;
		
		public threadBroadcaster(Message m) {
			this.m = m;
		}
		
		public void run() {
			LCBroadcast.this.broadcast(m);
		}
	}
	
	
	public void finalize() {
		System.out.printf("%d >>>>>>>> Remaining: %d Messages Attempting delivery\n",this.id,pending.size());
		//deliverPending(vectorClock.clone(), pending);
	}
	
}
