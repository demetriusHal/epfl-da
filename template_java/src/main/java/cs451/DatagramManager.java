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
    
	static boolean debug = false;
	static int timeout = 10	;
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
        //executor.execute(l);
        l.start();
        
        

    }

    public void send(Message m, int id) {
        //preprocces
        id = id-1;
        //System.out.println(id);
        Host host = hosts.get(id);
        InetAddress addr;
        int port = host.getPort();
        
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
            this.m = m;
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
            while(acked.get(m) == false) {
                //System.out.println("Sender> Sending a message");
                try {
                    DatagramManager.this.outgoing.send(pack);
                    Thread.sleep(1000);
                }catch(Exception e) {
                    e.printStackTrace();
                }
                
                time += 1;
            }
            //System.out.println("Sender> Received ack for message");
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
                //System.out.println("Listener> Thread Reloaded");
                DatagramPacket dgram = new DatagramPacket(new byte[listenSize], listenSize);
                try {
					receiving.receive(dgram);
				} catch (IOException e) {
					e.printStackTrace();
                }
                //System.out.println("Listener> Received a Datagram");
                Processor p = new Processor(dgram);
                //p.start();
                executor.execute(p);
            
            }
            //System.out.println("Listener> Thread Stopped");

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
            if (m.isAck == 1) {
                DatagramManager.this.modifyMap(m , true);
            } else {
                
                Message ackM = Message.ackMessage(m);
                byte[] buf = ackM.bytes();
                //System.out.println(m.sender);
                InetAddress addr = addrs[m.sender-1];
                int port = DatagramManager.this.hosts.get(m.sender-1).getPort();

                DatagramPacket dgram = new DatagramPacket(buf, buf.length, addr, port);
                //System.out.println(port+" " + DatagramManager.this.port);
                //create thread to send
                Sender s = new Sender(dgram);
                //s.start();
                executor.execute(s);
                
                //deliver message above
                DatagramManager.this.cb.deliver(m, m.sender);
                if (DatagramManager.debug)
                	System.out.printf("Processor> Delivered %d by %d\n", m.sequenceNum,m.sender);
            }
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
        	executor.awaitTermination(DatagramManager.timeout, TimeUnit.SECONDS);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	l.interrupt();
    }

}
