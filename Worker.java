import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Worker {

    private static int serverPort;
    private static String reducerHost;
    private static int reducerPort;
    private static final Map<String, Store> stores = new HashMap<>();

    public static void main(String[] args) {
        init();
        startServer();
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("worker.config"));

            serverPort = Integer.parseInt(prop.getProperty("serverPort"));
            reducerHost = prop.getProperty("reducerHost");
            reducerPort = Integer.parseInt(prop.getProperty("reducerPort"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
            System.out.println("Worker listening on port " + serverPort);
            while (true) {
                Socket masterSocket = serverSocket.accept();
                new Thread(() -> handleRequest(masterSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleRequest(Socket masterSocket) {
        try (ObjectInputStream in = new ObjectInputStream(masterSocket.getInputStream())) {
            while (true) {
                Chunk chunk = (Chunk) in.readObject();
                switch (chunk.getTypeID()) {
                    case 1 -> handleInsert(chunk);
                    case 2, 3 -> {
                        Socket reducerSocket = new Socket(reducerHost, reducerPort);
                        Thread workerTask = new ActionsForWorkers(chunk, stores, reducerSocket);
                        workerTask.start();
                    }
                    default -> System.out.println("Unknown typeID: " + chunk.getTypeID());
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void handleInsert(Chunk chunk) {
        Store store = (Store) chunk.getData();
        synchronized (stores) {
            stores.put(store.getStoreName(), store);
            System.out.println("Inserted store: " + store.getStoreName());
        }
    }
}
