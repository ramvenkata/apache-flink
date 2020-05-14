/**
 * 
 */
package org.practice.flink;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Ramesh
 *
 */
public class SampleDataStream {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		ServerSocket listener = new ServerSocket(8989);
		try {
			Socket socket = listener.accept();
			System.out.println("Got new connection: " + socket.toString());
			try {
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

				for (int i = 1; i < 100; i++) {

					int key = (i % 2) / 1;
					String s = new StringBuilder().append(key).append(",").append(i).toString();
					System.out.println("Socket Stream Data -> " + s);
					out.println(s);
					Thread.sleep(50);
				}

			} finally {
				socket.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			listener.close();
		}
	}

}
