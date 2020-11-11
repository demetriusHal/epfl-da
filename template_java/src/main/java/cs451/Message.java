package cs451;
//class that contains all informations regarding messages

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;


public class Message {

	static AtomicInteger seqGlobal = new AtomicInteger(0);
	//header info
	public byte sender;
	public byte receiver;
	public int sequenceNum;
	public byte[] data;

	public byte isAck = 0;

	static public Message  ackMessage(Message m) {

		return new Message(m.sequenceNum, m.receiver, m.sender, ((byte)0x1));

	}


	public Message(int sequenceNumber ,byte sender, byte receiver, byte isAck) {
		this.sequenceNum = sequenceNumber;
		this.sender = sender;
		this.receiver = receiver;
		this.isAck = isAck;
		
	}

	public Message(byte sender, byte receiver) {
		this.sequenceNum = seqGlobal.getAndIncrement();
		this.sender = sender;
		this.receiver = receiver;
		this.isAck = 0;
	}

	

	public Message(byte[] received) {
		byte[] msg = received.clone();
		ByteBuffer bb = ByteBuffer.wrap(msg);
		bb.order(ByteOrder.BIG_ENDIAN);

		this.sequenceNum = bb.getInt();
		this.sender = bb.get();
		this.receiver = bb.get();
		this.isAck = bb.get();
		this.data = Arrays.copyOfRange(bb.array(), 7, msg.length);
		
	}

	public Message(byte[] received, int length) {
		byte[] msg = received.clone();
		ByteBuffer bb = ByteBuffer.wrap(msg);
		bb.order(ByteOrder.BIG_ENDIAN);

		this.sequenceNum = bb.getInt();
		this.sender = bb.get();
		this.receiver = bb.get();
		this.isAck = bb.get();
		this.data = Arrays.copyOfRange(bb.array(), 7, length);
		
	}

	
	public void setData(byte[] data) {
		this.data = data.clone();
	}

	public void setData(String s) {
		byte[] b = s.getBytes();
		this.data = b.clone();
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
			length = Integer.max(data.length+7, 16);

		byte[] bytes = new byte[length];

		ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.order(ByteOrder.BIG_ENDIAN);


		bb.putInt(sequenceNum);
		bb.put(sender);
		bb.put(receiver);
		bb.put(isAck);
		if (data != null)
			bb.put(data);
		

		return bytes;


	}


	public String toString() {
		String rv = String.format("#%d\nfrom:%d\nto:%d\nVal:%s\n",
		sequenceNum, sender, receiver,new String(data, StandardCharsets.UTF_8));

		return rv;
	}

	public DatagramPacket wrap() {
		byte[] buf = bytes();
		return  new DatagramPacket(buf, buf.length);
	}


}