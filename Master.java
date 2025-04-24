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

    private static void sendToClient(Chunk result) {
        int segmentId = result.getSegmentID();
        Socket userSocket = userSockets.get(segmentId);
        if (userSocket != null) {
            try {
                ObjectOutputStream out = new ObjectOutputStream(userSocket.getOutputStream());
                out.writeObject(result);
                out.flush();
                userSocket.close();
                userSockets.remove(segmentId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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

    private static List<Store> allStores = new ArrayList<>();

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
                case 1 -> { // INSERT STORE
                    Store store = (Store) chunk.getData();
                    allStores.add(store);
                    System.out.println("✅ Store inserted: " + store.getStoreName());

                    // Ανάθεση σε Worker
                    int index = Math.abs(store.getStoreName().hashCode()) % numberOfWorkers;
                    workerOutputs.get(index).writeObject(chunk);
                    workerOutputs.get(index).flush();
                }

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

                case 4 -> { // UPDATE PRODUCT AVAILABILITY
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    String productName = (String) data.get("productName");
                    int newAmount = (int) data.get("newAmount");

                    for (Store s : allStores) {
                        if (s.getStoreName().equalsIgnoreCase(storeName)) {
                            for (Product p : s.getProducts()) {
                                if (p.getProductName().equalsIgnoreCase(productName)) {
                                    p.setAvailableAmount(newAmount);
                                    System.out.println("✅ Updated " + productName + " in " + storeName);
                                }
                            }
                        }
                    }

                    // Optionally notify Manager
                    Chunk response = new Chunk("admin", 4, "✔ Updated product availability.");
                    response.setSegmentID(segmentId);
                    sendToClient(response);
                }

                case 5 -> { // SEARCH STORE BY NAME
                    String name = (String) chunk.getData();
                    List<Store> found = new ArrayList<>();

                    for (Store s : allStores) {
                        if (s.getStoreName().equalsIgnoreCase(name)) {
                            found.add(s);
                        }
                    }

                    Chunk response = new Chunk("admin", 5, found);
                    response.setSegmentID(segmentId);
                    sendToClient(response);
                }

                case 6 -> { // STATISTICS
                    int totalStores = allStores.size();
                    int totalProducts = allStores.stream().mapToInt(s -> s.getProducts().size()).sum();
                    double avgStars = allStores.stream().mapToInt(Store::getStars).average().orElse(0);

                    Map<String, Object> stats = new HashMap<>();
                    stats.put("Total Stores", totalStores);
                    stats.put("Total Products", totalProducts);
                    stats.put("Average Stars", avgStars);

                    Chunk response = new Chunk("admin", 6, stats);
                    response.setSegmentID(segmentId);
                    sendToClient(response);
                }

                default -> System.out.println("❌ Unknown request type: " + chunk.getTypeID());
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


