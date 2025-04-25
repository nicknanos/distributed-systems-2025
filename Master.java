import java.io.*;
import java.net.*;
import java.util.*;

public class Master {
    private static int userPort;
    private static int reducerPort;
    private static int numberOfWorkers;

    private static Map<Integer, Socket> userSockets = new HashMap<>();
    private static int segmentIdCounter = 0;

    private static List<Store> allStores = new ArrayList<>();
    private static Map<String, Integer> salesByProduct = new HashMap<>();
    private static Map<String, Integer> salesByStoreType = new HashMap<>();
    private static Map<String, Integer> salesByProductCategory = new HashMap<>();

    public static void main(String[] args) {
        init();
        listenForUsers();
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));
            numberOfWorkers = Integer.parseInt(prop.getProperty("numberOfWorkers"));
            userPort = Integer.parseInt(prop.getProperty("userPort"));
            reducerPort = Integer.parseInt(prop.getProperty("reducerPort"));
            System.out.println("🚀 Master initialized.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listenForUsers() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(userPort)) {
                System.out.println("📡 Master listening on port " + userPort);
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
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            Chunk chunk = (Chunk) in.readObject();
            int segmentId;
            synchronized (Master.class) {
                segmentId = ++segmentIdCounter;
            }
            chunk.setSegmentID(segmentId);
            userSockets.put(segmentId, socket);

            switch (chunk.getTypeID()) {
                case 1 -> { // Προσθήκη καταστήματος
                    Store store = (Store) chunk.getData();
                    allStores.add(store);
                    int workerIndex = Math.abs(store.getStoreName().hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 1, "✅ Το κατάστημα '" + store.getStoreName() + "' προστέθηκε με επιτυχία."));
                }

                case 2 -> { // Ενημέρωση διαθεσιμότητας
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;

                    sendChunkToWorker(chunk, workerIndex);
                    out.writeObject(new Chunk("master", 2, "🔁 Το απόθεμα του προϊόντος ενημερώθηκε (στάλθηκε στον Worker)."));
                }

                case 3 -> { // Προσθήκη νέου προϊόντος
                    Map<String, Object> data = (Map<String, Object>) chunk.getData();
                    String storeName = (String) data.get("storeName");
                    Product product = (Product) data.get("product");

                    int workerIndex = Math.abs(storeName.hashCode()) % numberOfWorkers;
                    sendChunkToWorker(chunk, workerIndex);

                    out.writeObject(new Chunk("master", 3, "✅ Προϊόν προστέθηκε."));
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
                    out.writeObject(new Chunk("master", 5, salesByProduct));
                }

                case 6 -> { // Πωλήσεις ανά τύπο καταστήματος
                    out.writeObject(new Chunk("master", 6, salesByStoreType));
                }

                case 7 -> { // Πωλήσεις ανά κατηγορία προϊόντος
                    out.writeObject(new Chunk("master", 7, salesByProductCategory));
                }

                default -> {
                    out.writeObject(new Chunk("master", -1, "❌ Μη υποστηριζόμενη εντολή."));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendChunkToWorker(Chunk chunk, int workerIndex) {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("master.config"));

            String host = prop.getProperty("host" + (workerIndex + 1));
            int port = Integer.parseInt(prop.getProperty("worker" + (workerIndex + 1) + "Port"));

            try (Socket workerSocket = new Socket(host, port);
                 ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream())) {

                out.writeObject(chunk);
                out.flush();
                System.out.println("📦 Chunk sent to Worker " + (workerIndex + 1));

            }
        } catch (IOException e) {
            System.err.println("❌ Failed to send chunk to Worker " + (workerIndex + 1));
            e.printStackTrace();
        }
    }
}
