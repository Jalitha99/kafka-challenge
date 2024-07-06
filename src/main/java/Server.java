import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    private static final int PORT = 12345;
    private static final String OUTPUT_FILE = "final_counts.csv";
    private static final int[] finalCounts = new int[1000001];

    public static void main(String[] args) {
        int request_count = 0;

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            long startTime = System.currentTimeMillis(); // Start timing
            System.out.println("Server started, waiting for connections...");

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())) {


                    int[] receivedCounts = (int[]) in.readObject();
                    request_count+=1;
                    System.out.println("Request count: "+request_count);

                    // Increment finalCounts with receivedCounts
                    synchronized (finalCounts) {
                        for (int i = 1; i < finalCounts.length; i++) {
                            finalCounts[i] += receivedCounts[i];
                        }
                    }

                    if (request_count == 4) {
                        long endTime = System.currentTimeMillis(); // End timing
                        long duration = endTime - startTime; // Calculate duration

                        System.out.println("Time taken to process request and save CSV: " + duration + " milliseconds");
                        saveCountsToCsv(finalCounts);
                    }



                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void saveCountsToCsv(int[] counts) {
        try (FileWriter writer = new FileWriter(OUTPUT_FILE)) {
            for (int i = 1; i < counts.length; i++) { // Start from 1 since 0 is not used
                if (counts[i] > 0) {
                    writer.write(i + "," + counts[i] + "\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

