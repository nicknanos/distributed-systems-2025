
import java.io.*;
import java.net.*;
import java.util.*;
/**
 * Η κλάση Master αποτελεί τον κεντρικό διαχειριστή του κατανεμημένου συστήματος.
 * Αποδέχεται αιτήματα από πελάτες και διαχειριστές μέσω TCP συνδέσεων, τα αναθέτει σε Workers με χρήση hash-based κατανομής
 * και συλλέγει/συνενώνει τα αποτελέσματα.
 * Υλοποιεί MapReduce-style ροές για αναζητήσεις, και διαχειρίζεται τις λειτουργίες όπως αγορά προϊόντων, ενημερώσεις καταστημάτων,
 * στατιστικά πωλήσεων και βαθμολόγηση.
 */
record WorkerInfo(String host, int port) {}

public class Master {
    private static final boolean DEBUG_MODE = true;

    private static List<WorkerInfo> workers = new ArrayList<>();
    private static int userPort;
    private static int reducerPort;
    private static int numberOfWorkers;

    private static final Map<Integer, Socket> userSockets = new HashMap<>();
    private static final Map<Integer, ObjectOutputStream> userOutputStreams = new HashMap<>();
    private static final Map<Integer, Map<String, Integer>> partialProductSales = new HashMap<>();
    private static final Map<Integer, Integer> responsesReceived = new HashMap<>();
    private static final Map<Integer, List<Store>> pendingResults = new HashMap<>();

    private static int segmentIdCounter = 0;
    private static final List<Store> allStores = new ArrayList<>();

    public static void main(String[] args) {
        init();
        listenForUsers();
        listenForWorkerResponses();
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));
            numberOfWorkers = Integer.parseInt(prop.getProperty("numberOfWorkers"));
            userPort = Integer.parseInt(prop.getProperty("userPort"));
            reducerPort = Integer.parseInt(prop.getProperty("reducerPort"));

            for (int i = 1; i <= numberOfWorkers; i++) {
                String host = prop.getProperty("host" + i);
                int port = Integer.parseInt(prop.getProperty("worker" + i + "Port"));
                workers.add(new WorkerInfo(host, port));
            }

            println("Master initialized with " + numberOfWorkers + " workers.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listenForUsers() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(userPort)) {
                println("📡 Master listening on port " + userPort);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> handleUser(clientSocket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void handleUser(Socket socket) {
        Socket userSocket = socket;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(userSocket.getInputStream());
            out = new ObjectOutputStream(userSocket.getOutputStream());

            Chunk chunk = (Chunk) in.readObject();
            int segmentId;
            synchronized (Master.class) {
                segmentId = ++segmentIdCounter;
            }
            chunk.setSegmentID(segmentId);
            userSockets.put(segmentId, socket);
            userOutputStreams.put(segmentId, out);

            switch (chunk.getTypeID()) {
                case 1 -> {
                    Store store = (Store) chunk.getData();
                    allStores.add(store);
                    int workerIndex = getWorkerIndexForStore(store.getStoreName());
                    sendChunkToWorker(chunk, workerIndex);
                    out.writeObject(new Chunk("master", 1, "Το κατάστημα '" + store.getStoreName() + "' προστέθηκε."));
                }
                case 2, 3, 4 -> {
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    int workerIndex = getWorkerIndexForStore(storeName);
                    sendChunkToWorker(chunk, workerIndex);
                    out.writeObject(new Chunk("master", chunk.getTypeID(), "Η ενέργεια εκτελέστηκε."));
                }
                case 5 -> {
                    responsesReceived.put(segmentId, 0);
                    partialProductSales.put(segmentId, new HashMap<>());
                    for (int i = 0; i < numberOfWorkers; i++) {
                        Chunk requestChunk = new Chunk("admin", 5, null);
                        requestChunk.setSegmentID(segmentId);
                        sendChunkToWorker(requestChunk, i);
                    }
                }
                case 6, 7 -> {
                    for (int i = 0; i < numberOfWorkers; i++) {
                        Chunk requestChunk = new Chunk("admin", chunk.getTypeID(), null);
                        requestChunk.setSegmentID(segmentId);
                        sendChunkToWorker(requestChunk, i);
                    }
                }
                case 10 -> {
                    println("Νέα αναζήτηση...");
                    for (int i = 0; i < numberOfWorkers; i++) {
                        Chunk searchChunk = new Chunk("client", 10, chunk.getData());
                        searchChunk.setSegmentID(segmentId);
                        sendChunkToWorker(searchChunk, i);
                    }
                }
                case 11 -> { //Αγορά προιόντος
                    BuyRequest req = (BuyRequest) chunk.getData();
                    int workerIndex = Math.abs(req.getStoreName().hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 11, "Αγορά αιτήθηκε για " + req.getQuantity() + "x " + req.getProductName()));
                }

                case 12 -> { //Βαθμολόγηση καταστήματος
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 12, "Βαθμολογία κατοχυρώθηκε"));
                }

                default -> out.writeObject(new Chunk("master", -1, "Άγνωστη εντολή."));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listenForWorkerResponses() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(reducerPort)) {
                println("Master listening for Worker responses on port " + reducerPort);
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> handleWorkerResponse(socket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void handleWorkerResponse(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Chunk chunk = (Chunk) in.readObject();
            int segmentId = chunk.getSegmentID();

            switch (chunk.getTypeID()) {
                case 5, 6, 7, 11, 12 -> {
                    Map<String, Integer> part = (Map<String, Integer>) chunk.getData();
                    synchronized (Master.class) {
                        partialProductSales.putIfAbsent(segmentId, new HashMap<>());
                        Map<String, Integer> current = partialProductSales.get(segmentId);
                        for (var e : part.entrySet()) {
                            current.merge(e.getKey(), e.getValue(), Integer::sum);
                        }
                        int received = responsesReceived.merge(segmentId, 1, Integer::sum);
                        if (received == numberOfWorkers) {
                            sendResultsToUser(segmentId, current, chunk.getTypeID());
                        }
                    }
                }
                case 10 -> {
                    List<Store> stores = (List<Store>) chunk.getData();
                    synchronized (Master.class) {
                        pendingResults.putIfAbsent(segmentId, new ArrayList<>());
                        pendingResults.get(segmentId).addAll(stores);
                        int received = responsesReceived.merge(segmentId, 1, Integer::sum);
                        if (received == numberOfWorkers) {
                            sendResultsToUser(segmentId, pendingResults.get(segmentId), 10);
                            pendingResults.remove(segmentId);
                        }
                    }
                }

                default -> println("Άγνωστο typeID από Worker: " + chunk.getTypeID());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendChunkToWorker(Chunk chunk, int workerIndex) {
        try {
            WorkerInfo w = workers.get(workerIndex);
            try (Socket socket = new Socket(w.host(), w.port());
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                out.writeObject(chunk);
                out.flush();
                println("Chunk sent to Worker " + (workerIndex + 1));
            }
        } catch (IOException e) {
            System.err.println("Failed to send chunk to Worker " + (workerIndex + 1));
            e.printStackTrace();
        }
    }

    private static int getWorkerIndexForStore(String storeName) {
        return Math.abs(storeName.hashCode()) % numberOfWorkers;
    }

    private static void sendResultsToUser(int segmentId, Object resultData, int typeID) {
        try {
            Socket userSocket = userSockets.get(segmentId);
            ObjectOutputStream out = userOutputStreams.get(segmentId);
            if (userSocket == null || userSocket.isClosed() || out == null) {
                println("Το socket του χρήστη είναι κλειστό, δεν μπορώ να στείλω απάντηση.");
                return;
            }
            Chunk response = new Chunk("master", typeID, resultData);
            response.setSegmentID(segmentId);
            out.writeObject(response);
            out.flush();
            println("Αποτελέσματα στάλθηκαν στον πελάτη για typeID " + typeID);
        } catch (IOException e) {
            System.err.println("Σφάλμα κατά την αποστολή των αποτελεσμάτων.");
            e.printStackTrace();
        } finally {
            userSockets.remove(segmentId);
            userOutputStreams.remove(segmentId);
            responsesReceived.remove(segmentId);
        }
    }

    private static void println(String msg) {
        if (DEBUG_MODE) System.out.println(msg);
    }
}