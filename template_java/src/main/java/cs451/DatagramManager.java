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

public class DatagramManager {
    
    final public int port;
    DatagramSocket outgoing;
    
    //ack map
    private HashMap<Message, Boolean> acked;
    public List<Host> hosts;

    public InetAddress[] addrs;
    public int[] ports;

    //callback
    Delivery cb;


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

        Listener l = new Listener();
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
            sender.start();
        } catch(IOException e) {
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


            while(acked.get(m) == false) {
                //System.out.println("Sender> Sending a message");
                try {
                    DatagramManager.this.outgoing.send(pack);
                    Thread.sleep(1000);
                }catch(Exception e) {
                    e.printStackTrace();
                }
                
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
            //System.out.println("Listener> Thread Started");
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
                p.start();

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
            if (m.isAck == 1) {
                DatagramManager.this.modifyMap(m , true);
            } else {
                
                Message ackM = Message.ackMessage(m);
                byte[] buf = ackM.bytes();
                System.out.println(m.sender);
                InetAddress addr = addrs[m.sender-1];
                int port = DatagramManager.this.hosts.get(m.sender-1).getPort();

                DatagramPacket dgram = new DatagramPacket(buf, buf.length, addr, port);
                //System.out.println(port+" " + DatagramManager.this.port);
                //create thread to send
                Sender s = new Sender(dgram);
                s.start();

                //deliver message above
                //TODO callback
                DatagramManager.this.cb.deliver(m, m.sender);

                //System.out.printf("Processor> Delivered %d by %d\n", m.sequenceNum,m.sender);
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

}
