package org.lumongo.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class StreamHelper {
	
	public static void copyChannel(final ReadableByteChannel source, final WritableByteChannel destination) throws IOException {
		final ByteBuffer buffer = ByteBuffer.allocateDirect(32 * 1024);
		
		while (source.read(buffer) != -1) {
			buffer.flip();
			destination.write(buffer);
			buffer.compact();
		}
		
		buffer.flip();
		
		while (buffer.hasRemaining()) {
			destination.write(buffer);
		}
	}
	
	public static void copyStream(final InputStream source, final OutputStream destination) throws IOException {
		final ReadableByteChannel inputChannel = Channels.newChannel(source);
		final WritableByteChannel outputChannel = Channels.newChannel(destination);
		
		try {
			copyChannel(inputChannel, outputChannel);
		}
		finally {
			try {
				inputChannel.close();
			}
			catch (Exception e) {
				
			}
			try {
				outputChannel.close();
			}
			catch (Exception e) {
				
			}
		}
	}
	
	public static byte[] getBytesFromStream(final InputStream source) throws IOException {
		try {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			
			int nRead;
			byte[] data = new byte[32 * 1024];
			
			while ((nRead = source.read(data, 0, data.length)) != -1) {
				buffer.write(data, 0, nRead);
			}
			
			buffer.flush();
			
			return buffer.toByteArray();
		}
		finally {
			source.close();
		}
	}
}
