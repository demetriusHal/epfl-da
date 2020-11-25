package cs451;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Main {
	
	
	static FileWriter mainWriter;
    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
        try {
	        mainWriter.flush();
	        //mainWriter.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
        //System.exit(0);
        
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }
    
    static void writeOutput(FileWriter fw, String s) {
    	try {
    		fw.write(s);
    	} catch(IOException e) {
    		e.printStackTrace();
    	}
    }

    public static void main(String[] args) throws Exception {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();
        
        
        List<Host> hosts = parser.hosts();
        int myid = parser.myId();
        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID is " + pid + ".");
        System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");

        System.out.println("My id is " + parser.myId() + ".");
        System.out.println("List of hosts is:");

        
        for (Host host: hosts) {
            System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
        }

        System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
        System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
        System.out.println("Output: " + parser.output());
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
        }
        
        


        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());
    //read first line to get m
    BufferedReader reader = new BufferedReader(new FileReader(parser.config()));
    int nMessages = Integer.parseInt(reader.readLine());


	System.out.println("Broadcasting messages...");
    //TODO
	int myport = hosts.get(myid-1).getPort();
    
	
	//initialize output writer;
	final FileWriter myWriter = new FileWriter(String.format(parser.output(), myid));
	mainWriter = myWriter;

	Delivery callback = new Delivery() {
		public void deliver(Message m, int from) {
			//System.out.printf("Final> Delivered %d by %d\n", m.sequenceNum, from);
			//important the sequence num starts from 1
			Main.writeOutput(myWriter, String.format("d %d %d\n", from, m.sequenceNum+1));
		}
	};
	FIFOBroadcast fifo = new FIFOBroadcast(myid, myport, hosts, callback);
    //PerfectLink pl = new PerfectLink(myport, hosts, callback);
	
	System.out.println("Waiting for all processes for finish initialization");
    coordinator.waitOnBarrier();
	
	//int m = Integer.parseInt(configLines.get(0));
	for (int i=0; i < nMessages; i++) {
        Message m = new Message((byte)myid, (byte)0);
        fifo.broadcast(m);
        Main.writeOutput(myWriter, String.format("b %d\n", i+1));
        
        //pl.send(m, 3-myid);
    }
    
    //fifo.finalize();
    myWriter.flush();


    

    
    
    

	System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

	while (true) {
	    // Sleep for 1 hour
	    Thread.sleep(60 * 60 * 1000);
	}
    }
}
