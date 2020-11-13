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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DatagramManager {
	
	
	//this class is the core message sending class
	// Listener : always has a thread alive listening for messages
	// Processors: recieve a message from listener and processes
	// Sender : send messages once (used for acks)
	// Persistent Sender: Sends messages continuously until they get interrupted by an ACK
	
	
	//These messages contain all the details (Sender, originalSender, Sequence, Destination)
    
	static boolean debug = false;
	static int timeout = 5	;
	static boolean waitAllThreads = true;
	
	
    final public int port;
    DatagramSocket outgoing;
    
    //ack map
    private HashMap<Message, Boolean> acked;
    public List<Host> hosts;

    public InetAddress[] addrs;
    public int[] ports;

    //callback
    Delivery cb;
    
    //listener
    Listener l;
    
    //
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    ThreadPoolExecutor importantExector = (ThreadPoolExecutor) Executors.newCachedThreadPool();


    DatagramManager(int port, List<Host> hlist, Delivery callback) {
        this.cb = callback;
        this.port = port;
        this.hosts = hlist;
        this.addrs = new InetAddress[hosts.size()];
        this.ports = new int[hosts.size()];
        try {
            for (int i=0;i < hosts.size(); i++) {
                addrs[i] = InetAddress.getByName(hosts.get(i).getIp());
                ports[i] = hosts.get(i).getPort();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        acked = new HashMap<>();


        try {
            outgoing = new DatagramSocket();
        } catch(IOException e) {
            e.printStackTrace();
        }

        l = new Listener();
        l.start();
        
        

    }

    public void send(Message m, int id) {
        //preprocces
        id = id-1;
        //System.out.println(id);
        Host host = hosts.get(id);
        InetAddress addr;
        int port = host.getPort();
        
        //add destination to differentiate
        m.receiver = (byte)(id+1);
        try {
            addr = InetAddress.getByName(host.getIp());
            PersistentSender sender = new PersistentSender(m, addr, port);
            //sender.start();
            executor.execute(sender);
        } catch(Exception e) {
            e.printStackTrace();
        }
        
    }

    public class PersistentSender extends Thread {
        Message m;
        InetAddress addr;
        int port;

        PersistentSender(Message m, InetAddress addr, int port) {
            this.m = m.clone();
            this.addr = addr;
            this.port = port;
        }

        public void run() {

            DatagramManager.this.modifyMap(m, false);

            DatagramPacket pack = m.wrap();
            pack.setAddress(addr);
            pack.setPort(port);

            //System.out.println("Sender> Started!");

            int time = 0;
            int sleeptime = 1000;
            while(acked.get(m) == false) {
            	if (DatagramManager.debug)
            		System.out.printf("Sender %d> Sending a message %d %d %d %d\n", Thread.currentThread().getId(),m.sequenceNum, m.from, m.sender, m.receiver);
                try {
                    DatagramManager.this.outgoing.send(pack);
                    Thread.sleep(sleeptime);
                }catch(Exception e) {
                    e.printStackTrace();
                }
                
                sleeptime *= 2;
                time += 1;
            }
            if (DatagramManager.debug)
            	System.out.println("Sender> Received ack for message");
        }
    }


    public class Listener extends Thread {
        final private int port ;
        private DatagramSocket receiving;

        final static int listenSize = 1024;

        public Listener() {
            this.port = DatagramManager.this.port;
            try {
                this.receiving = new DatagramSocket(port);
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            
            //here we listen for information
            while (!Thread.interrupted()) {

            	DatagramPacket dgram = new DatagramPacket(new byte[listenSize], listenSize);
                try {
					receiving.receive(dgram);
				} catch (IOException e) {
					e.printStackTrace();
                }

                Processor p = new Processor(dgram);
                //p.start();
                importantExector.execute(p);
            
            }

        }

    }



    public class Processor extends Thread {
        
        private DatagramPacket packet;

        Processor(DatagramPacket packet) {
            this.packet = packet;
        }

        public void run() {
            Message m = new Message(packet.getData(), packet.getLength());
            
            //if message is ack then add it to the hash table
            
            	//System.out.printf("Processor> %d Created\n",Thread.currentThread().getId(), m.sequenceNum,m.sender);
            if (m.isAck == 1) {
            	if (DatagramManager.debug)
            		System.out.printf("Ack received! for %d %d %d %d\n", m.sequenceNum, m.from, m.sender, m.receiver);
                DatagramManager.this.modifyMap(m , true);
            } else {
            	if (DatagramManager.debug)
            		System.out.printf("Message received! for %d %d %d %d\n", m.sequenceNum, m.from, m.sender, m.receiver);
                Message ackM = Message.ackMessage(m);
                byte[] buf = ackM.bytes();

                InetAddress addr = addrs[m.sender-1];
                int port = DatagramManager.this.hosts.get(m.sender-1).getPort();

                DatagramPacket dgram = new DatagramPacket(buf, buf.length, addr, port);

                if (DatagramManager.debug)
                	System.out.printf("Sending ack for %d %d %d %d\n", ackM.sequenceNum, ackM.from, ackM.sender, ackM.receiver);

                Sender s = new Sender(dgram);

                importantExector.execute(s);
                
                //deliver message above
                // before delivering, remove unnecessary information.
                //!!! IMPORTANT
                m.receiver = 0;
                DatagramManager.this.cb.deliver(m, m.sender);
                if (DatagramManager.debug)
                	System.out.printf("Processor> Delivered %d by %d\n", m.sequenceNum,m.sender);
            }
            //System.out.printf("Processor> Finished %d\n", Thread.currentThread().getId());
        }
    }

    public class Sender extends Thread {
        DatagramPacket dgram;

        Sender(DatagramPacket dgram) {
            this.dgram = dgram;
        }

        public void run() {
            try {
                DatagramManager.this.outgoing.send(dgram);
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }
 
    synchronized void modifyMap(Message m, boolean b) {
        acked.put(m, b);
    }
    
    public void finalize() {
    	try {
    		int oldtasks = 0;
    		//check if no new tasks are being created and stop then.
    		for (int j=0; j < 60; j++) {
    			int newtasks = (int)(importantExector.getTaskCount()-oldtasks);
    			System.out.println(j+ " Master executor:" + newtasks);
    			oldtasks = (int)importantExector.getTaskCount();
    			Thread.sleep(1000);
    			if (newtasks == 0)
    				break;
    		}
    		
//        	boolean b = importantExector.awaitTermination(30, TimeUnit.SECONDS);
//        	if (!b)
//        		System.err.println("Reached TImeoutNot all threads finished...");
        	//Thread.sleep(DatagramManager.timeout*1000);
        	importantExector.shutdownNow();

    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	l.interrupt();
    }

}
