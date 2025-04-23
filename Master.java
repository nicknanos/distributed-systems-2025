import java.io.*;
import java.net.*;
import java.util.*;

public class Master {
    private static int userPort;
    private static int reducerPort;
    private static int numberOfWorkers;

    private static List<ObjectOutputStream> workerOutputs = new ArrayList<>();
    private static Map<Integer, Socket> userSockets = new HashMap<>();
    private static Map<Integer, Integer> responsesReceived = new HashMap<>();
    private static Map<Integer, List<Store>> partialResults = new HashMap<>();
    private static int segmentIdCounter = 0;

    public static void main(String[] args) {
        init();
        connectToWorkers();
        listenForUsers();
        listenForReducer();
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));

            numberOfWorkers = Integer.parseInt(prop.getProperty("numberOfWorkers"));
            userPort = Integer.parseInt(prop.getProperty("userPort"));
            reducerPort = Integer.parseInt(prop.getProperty("reducerPort"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void connectToWorkers() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));

            for (int i = 1; i <= numberOfWorkers; i++) {
                String host = prop.getProperty("host" + i);
                int port = Integer.parseInt(prop.getProperty("worker" + i + "Port"));
                Socket workerSocket = new Socket(host, port);
                ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
                workerOutputs.add(out);
                System.out.println("Connected to Worker " + i);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listenForUsers() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(userPort)) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> handleUser(clientSocket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void listenForReducer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(reducerPort)) {
                while (true) {
                    Socket reducerSocket = serverSocket.accept();
                    new Thread(() -> handleReducerResponse(reducerSocket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void handleUser(Socket clientSocket) {
        try {
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
            Chunk chunk = (Chunk) in.readObject();

            int segmentId;
            synchronized (Master.class) {
                segmentId = ++segmentIdCounter;
            }
            chunk.setSegmentID(segmentId);
            userSockets.put(segmentId, clientSocket);

            switch (chunk.getTypeID()) {
                case 2 -> { // SEARCH
                    for (ObjectOutputStream out : workerOutputs) {
                        out.writeObject(chunk);
                        out.flush();
                    }
                }
                case 3 -> { // BUY
                    BuyRequest req = (BuyRequest) chunk.getData();
                    int workerIndex = Math.abs(req.getStoreName().hashCode()) % numberOfWorkers;
                    workerOutputs.get(workerIndex).writeObject(chunk);
                    workerOutputs.get(workerIndex).flush();
                }
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void handleReducerResponse(Socket reducerSocket) {
        try (ObjectInputStream in = new ObjectInputStream(reducerSocket.getInputStream())) {
            Chunk response = (Chunk) in.readObject();
            int segmentId = response.getSegmentID();
            Socket userSocket = userSockets.get(segmentId);
            if (userSocket != null) {
                ObjectOutputStream out = new ObjectOutputStream(userSocket.getOutputStream());
                out.writeObject(response);
                out.flush();
                userSocket.close();
                userSockets.remove(segmentId);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
