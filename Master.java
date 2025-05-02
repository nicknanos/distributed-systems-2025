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
public class Master {
    private static int userPort;
    private static int reducerPort;
    private static int numberOfWorkers;

    private static Map<Integer, Socket> userSockets = new HashMap<>();
    private static Map<Integer, ObjectOutputStream> userOutputs = new HashMap<>();
    private static int segmentIdCounter = 0;

    private static List<Store> allStores = new ArrayList<>();
    //private static Map<String, Integer> salesByProduct = new HashMap<>();
    //private static Map<String, Integer> salesByStoreType = new HashMap<>();
    //private static Map<String, Integer> salesByProductCategory = new HashMap<>();

    public static void main(String[] args) {
        init(); // Φόρτωση παραμέτρων από config
        listenForUsers(); // Άνοιγμα πόρτας για επικοινωνία με clients
        listenForWorkerResponses(); // Πόρτα για responses από Workers
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));
            numberOfWorkers = Integer.parseInt(prop.getProperty("numberOfWorkers"));
            userPort = Integer.parseInt(prop.getProperty("userPort"));
            reducerPort = Integer.parseInt(prop.getProperty("reducerPort"));
            System.out.println("Master initialized.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Δημιουργεί Thread που περιμένει συνδέσεις από Manager/Users
    private static void listenForUsers() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(userPort)) {
                System.out.println("Master listening on port " + userPort);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> handleUser(clientSocket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // Αντιμετωπίζει αίτημα από Manager ή DummyUser
    private static void handleUser(Socket socket) {
        try {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            Chunk chunk = (Chunk) in.readObject();
            int segmentId;
            synchronized (Master.class) {
                segmentId = ++segmentIdCounter;
            }
            chunk.setSegmentID(segmentId);

            // Κρατάμε το Socket και το OutputStream!
            userSockets.put(segmentId, socket);
            userOutputs.put(segmentId, out);

            switch (chunk.getTypeID()) {
                case 1 -> { // Προσθήκη καταστήματος
                    Store store = (Store) chunk.getData();
                    allStores.add(store);
                    int workerIndex = Math.abs(store.getStoreName().hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 1, "Το κατάστημα '" + store.getStoreName() + "' προστέθηκε με επιτυχία."));
                }

                case 2 -> { // Ενημέρωση διαθεσιμότητας
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;

                    sendChunkToWorker(chunk, workerIndex);
                    out.writeObject(new Chunk("master", 2, "Το απόθεμα του προϊόντος ενημερώθηκε (στάλθηκε στον Worker)."));
                }

                case 3 -> { // Προσθήκη νέου προϊόντος
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    Product product = (Product) data.get("product");

                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 3, "Προϊόν προστέθηκε."));
                }

                case 4 -> { // Αφαίρεση προϊόντος
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    String productName = (String) data.get("productName");

                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 4, "🗑 Προϊόν αφαιρέθηκε."));
                }

                case 5 -> { // Πωλήσεις ανά προϊόν
                    for (int i = 0; i < numberOfWorkers; i++) {
                        sendChunkToWorker(chunk, i);  // Στέλνει σε κάθε Worker
                    }

                    Map<String, Integer> finalResult = new HashMap<>();

                    // Πάρε απαντήσεις από όλους τους Workers
                    for (int i = 0; i < numberOfWorkers; i++) {
                        try {
                            Properties prop = new Properties();
                            prop.load(new FileInputStream("master.config"));

                            String host = prop.getProperty("host" + (i + 1));
                            int port = Integer.parseInt(prop.getProperty("worker" + (i + 1) + "Port"));

                            try (Socket workerSocket = new Socket(host, port);
                                 ObjectInputStream in1 = new ObjectInputStream(workerSocket.getInputStream())) {

                                Chunk workerResponse = (Chunk) in1.readObject();
                                Map<String, Integer> workerSales = (Map<String, Integer>) workerResponse.getData();

                                // Μαζεύουμε τα αποτελέσματα
                                for (Map.Entry<String, Integer> entry : workerSales.entrySet()) {
                                    finalResult.merge(entry.getKey(), entry.getValue(), Integer::sum);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    // Στείλε στον Manager την συνολική λίστα
                    out.writeObject(new Chunk("master", 5, finalResult));
                    out.flush();
                }

                case 6 -> { // Πωλήσεις ανά τύπο καταστήματος   ----- Not in currently in use-----
                    Map<String, Integer> totalSalesByType = new HashMap<>();

                    // Στέλνουμε σε όλους τους Workers ένα Chunk με typeID 6
                    for (int i = 0; i < numberOfWorkers; i++) {
                        Chunk requestChunk = new Chunk("admin", 6, null);
                        sendChunkToWorker(requestChunk, i);
                    }

                    // Περιμένουμε αποτελέσματα από όλους
                    for (int i = 0; i < numberOfWorkers; i++) {
                        Map<String, Integer> partial = (Map<String, Integer>) receiveWorkerResponse();
                        for (Map.Entry<String, Integer> entry : partial.entrySet()) {
                            totalSalesByType.merge(entry.getKey(), entry.getValue(), Integer::sum);
                        }
                    }

                    out.writeObject(new Chunk("master", 6, totalSalesByType));
                    out.flush();
                }

                case 7 -> { // Πωλήσεις ανά κατηγορία προϊόντος ----- Not in currently in use-----
                    Map<String, Integer> totalSalesByCategory = new HashMap<>();

                    for (int i = 0; i < numberOfWorkers; i++) {
                        Chunk requestChunk = new Chunk("admin", 7, null);
                        sendChunkToWorker(requestChunk, i);
                    }

                    for (int i = 0; i < numberOfWorkers; i++) {
                        Map<String, Integer> partial = (Map<String, Integer>) receiveWorkerResponse();
                        for (Map.Entry<String, Integer> entry : partial.entrySet()) {
                            totalSalesByCategory.merge(entry.getKey(), entry.getValue(), Integer::sum);
                        }
                    }

                    out.writeObject(new Chunk("master", 7, totalSalesByCategory));
                    out.flush();
                }

                case 10 -> { //Αναζήτηση προιόντος
                    int requestId = segmentId;
                    System.out.println("Νέα αναζήτηση...");

                    userSockets.put(requestId, socket);

                    for (int i = 0; i < numberOfWorkers; i++) {
                        sendChunkToWorker(chunk, i);
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

                    out.writeObject(new Chunk("master", 12, "Βαθμολογία."));
                }


                default -> {
                    out.writeObject(new Chunk("master", -1, "Μη υποστηριζόμενη εντολή."));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Στέλνει ένα αντικείμενο τύπου Chunk στον κατάλληλο Worker.
     * Η επιλογή του Worker γίνεται βάσει του index που υπολογίζεται από τον Master.
     *
     */
    private static void sendChunkToWorker(Chunk chunk, int workerIndex) {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));

            String host = prop.getProperty("host" + (workerIndex + 1));
            int port = Integer.parseInt(prop.getProperty("worker" + (workerIndex + 1) + "Port"));

            // Δημιουργία σύνδεσης προς τον Worker
            try (Socket workerSocket = new Socket(host, port);
                 ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream())) {

                // Αποστολή του αντικειμένου
                out.writeObject(chunk);
                out.flush();
                System.out.println("Chunk sent to Worker " + (workerIndex + 1));

            }
        } catch (IOException e) {
            System.err.println("Failed to send chunk to Worker " + (workerIndex + 1));
            e.printStackTrace();
        }
    }

    //Περιμένει να λάβει μία απάντηση από κάποιο Worker*
    private static Object receiveWorkerResponse() {
        try (ServerSocket serverSocket = new ServerSocket(0)) { // Χρησιμοποιεί ελεύθερο προσωρινό port
            int port = serverSocket.getLocalPort();
            System.out.println("Listening for Worker response on port " + port);

            try (Socket workerSocket = serverSocket.accept();
                 ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {

                Chunk chunk = (Chunk) in.readObject();
                System.out.println("Received data from Worker.");
                return chunk.getData();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Map<Integer, List<Store>> pendingResults = new HashMap<>();
    private static Map<Integer, Integer> responsesReceived = new HashMap<>();
    /**
     * Ανοίγει μια σταθερή πόρτα (reducerPort) μέσω της οποίας οι Workers στέλνουν
     * απαντήσεις προς τον Master για λειτουργίες όπως αναζήτηση (search).
     * Κάθε νέα σύνδεση εξυπηρετείται σε δικό της Thread.
     */
    private static void listenForWorkerResponses() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(reducerPort)) {
                System.out.println("Master listening for Worker responses on port " + reducerPort);
                while (true) {
                    Socket workerSocket = serverSocket.accept();
                    new Thread(() -> handleWorkerResponse(workerSocket)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
    /**
     * Λαμβάνει ένα αντικείμενο Chunk από έναν Worker και αποθηκεύει προσωρινά τα αποτελέσματα
     * μέχρι να απαντήσουν όλοι οι Workers. Τότε καλείται η sendResultsToUser.
     *
     */
    private static void handleWorkerResponse(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Chunk chunk = (Chunk) in.readObject();
            int segmentId = chunk.getSegmentID();
            List<Store> workerStores = (List<Store>) chunk.getData();

            synchronized (Master.class) {
                pendingResults.putIfAbsent(segmentId, new ArrayList<>());
                pendingResults.get(segmentId).addAll(workerStores);

                responsesReceived.put(segmentId, responsesReceived.getOrDefault(segmentId, 0) + 1);

                // Όταν έχουμε λάβει αποτελέσματα από ΟΛΟΥΣ τους Workers
                if (responsesReceived.get(segmentId) == numberOfWorkers) {
                    sendResultsToUser(segmentId, workerStores);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Στέλνει το τελικό αποτέλεσμα της αναζήτησης στον DummyUser.
     * Κλείνει και το socket αφού ολοκληρωθεί η αποστολή.
     */
    private static void sendResultsToUser(int segmentId, List<Store> results) {
        Socket socket = userSockets.get(segmentId);
        ObjectOutputStream out = userOutputs.get(segmentId);

        if (socket != null && out != null) {
            try {
                Chunk response = new Chunk("master", 10, results);
                response.setSegmentID(segmentId);

                out.writeObject(response);
                out.flush();

                socket.close(); // Μόνο ΤΩΡΑ κλείνουμε το socket.
                userSockets.remove(segmentId);
                userOutputs.remove(segmentId);

                System.out.println("Αποτελέσματα εστάλησαν στον DummyUser.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



}
