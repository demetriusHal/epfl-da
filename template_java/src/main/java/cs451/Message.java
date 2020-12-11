package cs451;
//class that contains all informations regarding messages

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;


public class Message {

	static int vcSize = 20;
	static AtomicInteger seqGlobal = new AtomicInteger(0);
	//header info
	public byte sender;
	public byte receiver;
	public 	int sequenceNum;
	public byte from;
	
	public byte[] data;
	
	
	//maybe use two bytes?
	public byte[] vectorClock;

	public byte isAck = 0;

	static public Message  ackMessage(Message m) {

		return new Message(m.sequenceNum, m.sender, m.receiver, m.from, ((byte)0x1));

	}

	public Message(int sequenceNumber ,byte sender, byte receiver, byte from,byte isAck) {
		this.sequenceNum = sequenceNumber;
		this.sender = sender;
		this.receiver = receiver;
		this.isAck = isAck;
		this.from = from;
		
		this.vectorClock = null;
		
	}

	public Message(int sequenceNumber ,byte sender, byte receiver, byte isAck) {
		this.sequenceNum = sequenceNumber;
		this.sender = sender;
		this.receiver = receiver;
		this.isAck = isAck;
		this.from = sender;
	
		this.vectorClock = null;

	}

	public Message(byte sender, byte receiver) {
		this.sequenceNum = seqGlobal.getAndIncrement();
		this.sender = sender;
		this.receiver = receiver;
		this.isAck = 0;
		this.from = sender;
		
		this.vectorClock = null;

	}

	

	public Message(byte[] received) {
		byte[] msg = received.clone();
		ByteBuffer bb = ByteBuffer.wrap(msg);
		bb.order(ByteOrder.BIG_ENDIAN);

		this.sequenceNum = bb.getInt();
		this.sender = bb.get();
		this.receiver = bb.get();
		this.isAck = bb.get();
		this.from = bb.get();
		this.vectorClock = Arrays.copyOfRange(bb.array(), 8,8+vcSize);
		this.data = Arrays.copyOfRange(bb.array(), 8+vcSize, msg.length+vcSize);
		
	}

	public Message(byte[] received, int length) {
		byte[] msg = received.clone();
		ByteBuffer bb = ByteBuffer.wrap(msg);
		bb.order(ByteOrder.BIG_ENDIAN);

		this.sequenceNum = bb.getInt();
		this.sender = bb.get();
		this.receiver = bb.get();
		this.isAck = bb.get();
		this.from = bb.get();
		this.vectorClock = Arrays.copyOfRange(bb.array(), 8,8+vcSize);
		this.data = Arrays.copyOfRange(bb.array(), 8+vcSize, length+vcSize);
		
	}

	
	public void setData(byte[] data) {
		this.data = data.clone();
	}

	public void setData(String s) {
		byte[] b = s.getBytes();
		this.data = b.clone();
	}
	
	public void setVC(byte[] vclock) {
		this.vectorClock = vclock;
	}


	@Override
	public boolean equals(Object o)  {
		if (o == this)
			return true;
		//this could crash
		Message m = (Message) o;

		if (m.sequenceNum != this.sequenceNum)
			return false;

		if (m.sender != this.sender)
			return false;
				
		if (m.from != this.from)
			return false;
		
		if (m.receiver != this.receiver)
			return false;
		return true;
	}

	@Override
	public int hashCode() {

		int p = 31;
		int hashcode = 13;

		//hashcode = hashcode*p + data.hashCode();
		hashcode = hashcode*p + sender;
		hashcode = hashcode*p + receiver;
		hashcode = hashcode*p + sequenceNum;
		hashcode = hashcode*p + from;

		return hashcode;
	}


	byte[] bytes() {
		//this is where C would be vastly superior
		// https://stackoverflow.com/questions/6891663/is-it-possible-to-use-struct-like-constructs-in-java
		// this is where I copied my code
		// miss the days where a simple cast would do the trick
		// assuming integer in 4 bytes
		int length;
		if (data == null)
			length = 16;
		else
			length = Integer.max(data.length+8, 16);

		byte[] bytes = new byte[length+vcSize*2];

		ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(ByteOrder.BIG_ENDIAN);


		bb.putInt(sequenceNum);		//4
		bb.put(sender);				//5
		bb.put(receiver);			//6
		bb.put(isAck);				//7
		bb.put(from);				//8
		if (vectorClock != null)
			bb.put(vectorClock);		//n
		if (data != null)
			bb.put(data);
		

		return bytes;


	}
	
	public Message clone() {
		Message mnew = new Message(this.sequenceNum, this.sender, this.receiver, this.isAck);
		mnew.from = this.from;
		mnew.data = this.data;
		if (this.vectorClock != null)
			mnew.vectorClock = this.vectorClock.clone();
		return mnew;
	}


	public String toString() {
		if (data == null)
			data = new byte[0];
		String rv = String.format("#%d\nfrom:%d\nto:%d\nVal:%s\n",
		sequenceNum, sender, receiver,new String(data, StandardCharsets.UTF_8));

		return rv;
	}

	public DatagramPacket wrap() {
		byte[] buf = bytes();
		return  new DatagramPacket(buf, buf.length);
	}


}