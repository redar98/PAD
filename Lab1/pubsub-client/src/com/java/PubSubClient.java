package com.java;

import java.io.*;
import java.net.Socket;

public class PubSubClient {

    private final static int port = 8787;
    private final static BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

    public static void main(String[] args) {
        System.out.println("Connecting to localhost:" + port);

        try (Socket clientSocket = new Socket("localhost", port)) {
            System.out.println("Connection established.");
            PrintWriter serverOut = new PrintWriter(clientSocket.getOutputStream(), true);
            listenOutput(clientSocket);

            for (String cmd = input.readLine(); !cmd.equalsIgnoreCase("quit"); cmd = input.readLine()) {
                serverOut.println(cmd);
            }

            System.out.println("Connection to the server closed.");
        } catch (IOException e) {
            System.out.println("An exception occurred while retrieving server data: " + e.getMessage());
        }
    }

    private static void listenOutput(Socket socket) {
        new Thread(() -> {
            try {
                InputStream in = socket.getInputStream();
                while (!socket.isClosed()) {
                    System.out.println(getResp(in));
                }
            } catch (IOException ignored) { }
        }).start();
    }

    private static String getResp(InputStream serverIn) {
        byte[] resp = new byte[0];
        try {
            resp = new byte[300];
            serverIn.read(resp);
        } catch (IOException ignored) {
        }
        return new String(resp);
    }
}
