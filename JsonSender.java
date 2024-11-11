package javaapplication1;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JsonSender {

    private static final String LOGSTASH_IP = "127.0.0.1"; // Replace with your Logstash IP
    private static final int LOGSTASH_PORT = 5044;         // Replace with your Logstash port

    public static void main(String[] args) {
        // Path to the JSON files
        String reqFilePath = "req_sample.json";
        String resFilePath = "C:\\ELK\\logstash-7.16.2-windows-x86_64\\logstash-7.16.2\\config\\res_sample.json";

        // Number of seconds the program should run
        int nSeconds = 200;

        try {
            // Read JSON data from both request and response files
            List<String> reqJsonMessages = readJsonFile(reqFilePath);
            List<String> resJsonMessages = readJsonFile(resFilePath);

            // Ensure there are enough messages by repeating if necessary
            reqJsonMessages = repeatMessages(reqJsonMessages, 200);
            resJsonMessages = repeatMessages(resJsonMessages, 200);

            // Send random messages to Logstash for `nSeconds` and track sent messages
            sendJsonForDuration(reqJsonMessages, resJsonMessages, nSeconds);
        } catch (IOException e) {
            System.err.println("Error reading JSON files: " + e.getMessage());
        }
    }

    // Read JSON file and return a list of JSON strings
    private static List<String> readJsonFile(String filePath) throws IOException {
        List<String> jsonMessages = new ArrayList<>();
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        
        // Assuming each line in the file is a separate JSON message
        // Split the content by lines to create individual JSON messages
        jsonMessages.addAll(Arrays.asList(content.split("\\n")));
        
        return jsonMessages;
    }

    // Repeat messages to ensure at least `numMessages` are available
    private static List<String> repeatMessages(List<String> messages, int numMessages) {
        List<String> repeatedMessages = new ArrayList<>(messages);

        // Repeat messages until there are enough to meet numMessages
        while (repeatedMessages.size() < numMessages) {
            repeatedMessages.addAll(messages);
        }

        // Trim the list to exactly numMessages if it exceeds the required count
        return repeatedMessages.subList(0, numMessages);
    }

    // Send random JSON messages to Logstash for the given duration (in seconds)
    private static void sendJsonForDuration(List<String> reqJsonMessages, List<String> resJsonMessages, int nSeconds) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (nSeconds * 1000); // Convert seconds to milliseconds

        int reqCount = 0; // Counter for request messages
        int resCount = 0; // Counter for response messages

        try (Socket socket = new Socket(LOGSTASH_IP, LOGSTASH_PORT); 
             OutputStream outputStream = socket.getOutputStream(); 
             PrintWriter writer = new PrintWriter(outputStream, true)) {

            // Combine both request and response JSON messages into one list
            List<String> combinedMessages = new ArrayList<>();
            combinedMessages.addAll(reqJsonMessages); // 200 from request
            combinedMessages.addAll(resJsonMessages); // 200 from response
            
            // Shuffle the combined list to randomize the order
            Collections.shuffle(combinedMessages);

            // Continuously send messages for the specified duration
            while (System.currentTimeMillis() < endTime) {
                // Get a random message from the shuffled list
                String message = combinedMessages.get(new Random().nextInt(combinedMessages.size()));
                
                // Send the message to Logstash
                writer.println(message);
                System.out.println("Sent message to Logstash.");

                // Increment counters based on message type
                if (message.contains("Request Json is :")) {
                    reqCount++;
                } else if (message.contains("Response Json is :")) {
                    resCount++;
                }

                // Sleep briefly to avoid overwhelming the Logstash server (adjust as needed)
                try {
                    Thread.sleep(100); // Sleep for 100ms between messages
                } catch (InterruptedException e) {
                    System.err.println("Error during sleep: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error sending JSON data to Logstash: " + e.getMessage());
        }

        // After the loop, print the message counts
        System.out.println("Total Request Messages Sent: " + reqCount);
        System.out.println("Total Response Messages Sent: " + resCount);
    }
}
