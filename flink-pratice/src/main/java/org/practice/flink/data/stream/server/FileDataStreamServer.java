/**
 * 
 */
package org.practice.flink.data.stream.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author Ramesh
 *
 */
public class FileDataStreamServer {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		ServerSocket listener = new ServerSocket(8989);

		Socket socket = null;
		try {
			socket = listener.accept();
			System.out.println("Got new connection: " + socket.toString());

			BufferedReader br = new BufferedReader(new FileReader("C:\\Root\\View\\Installations\\flink-1.9.2\\programs\\operators\\avg"));
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			String line;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				out.println(line);
				Thread.sleep(50);
			}

		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			if (socket != null) {
				socket.close();
			}
		}

		listener.close();
	}

}
