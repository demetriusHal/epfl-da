package cs451;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;

public class PerfectLink {

    public final int port;
    private final DatagramSocket sending;
    private final DatagramSocket receiving;


    //metadata
    HashSet<Message> delivered;
    


    public PerfectLink(int port) throws SocketException {
        this.port = port;
        sending = new DatagramSocket();
        receiving = new DatagramSocket(port);
        
        delivered = new HashSet<>();
    }


    public void send(Message m, InetAddress addr, int port) throws IOException{

        byte[] buf = m.bytes();

        DatagramPacket packet = new DatagramPacket(buf, buf.length, addr, port);
        int i = 0;
        while(true) {
            sending.send(packet);
            System.out.println("Sending m "+i++);
            
            try {Thread.sleep(1000);}
            catch(Exception e){;}
            

        }

    }

    private void ackMessage(DatagramPacket packet, Message m) throws IOException{

        Message ack = Message.ackMessage(m);
        byte[] buf = ack.bytes();

        DatagramPacket dgram = new DatagramPacket(buf, buf.length, packet.getSocketAddress());
        
        sending.send(dgram);
    }

    public Message receive() throws IOException{
        byte[] buf = new byte[1024];
        DatagramPacket received = new DatagramPacket(buf, 4096);

        int i =0;

        while(true) {
            receiving.receive(received);
            System.out.println(received.getLength());
            Message m = new Message(received.getData(), received.getLength());
            System.out.println("Received "+  i++);
            if (delivered.contains(m)) {
                ackMessage(received, m);
                continue;
            }
            else {
                delivered.add(m);
                //sent back ack to stop sending
                //
                ackMessage(received, m);

                
                System.out.println("Delivered m");
                return m;
            }

        }

        //return new Message(received.getData());
    }
}