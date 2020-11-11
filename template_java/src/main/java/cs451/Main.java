package cs451;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Main {

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
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

	System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

	System.out.println("Broadcasting messages...");
    //TODO

    

    DatagramManager mg = new DatagramManager(hosts.get(myid-1).getPort(), hosts);
    
    for (int i=0; i < 10; i++) {
        Message m = new Message((byte)myid, (byte)(3-myid));
        mg.send(m, 3-myid);
    }



    
    
    

	System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

	while (true) {
	    // Sleep for 1 hour
	    Thread.sleep(60 * 60 * 1000);
	}
    }
}
