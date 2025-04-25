import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Worker {
    private static int port;
    private static List<Store> storeList = Collections.synchronizedList(new ArrayList<>());

    // Πωλήσεις που θα ενημερώνει στον Master όταν ζητηθούν
    private static final Map<String, Integer> salesByProduct = new HashMap<>();
    private static final Map<String, Integer> salesByStoreType = new HashMap<>();
    private static final Map<String, Integer> salesByProductCategory = new HashMap<>();

    public static void main(String[] args) {
        init();
        listenForMaster();
    }

    private static void init() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("worker.config"));
            port = Integer.parseInt(prop.getProperty("serverPort"));
            System.out.println("👷 Worker listening on port " + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listenForMaster() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket masterSocket = serverSocket.accept();
                new Thread(() -> handleRequest(masterSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleRequest(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Chunk chunk = (Chunk) in.readObject();
            System.out.println("📥 Received Chunk (typeID=" + chunk.getTypeID() + ")");

            switch (chunk.getTypeID()) {
                case 1 -> handleInsertStore(chunk);
                case 2 -> handleUpdateAvailability(chunk);
                case 3 -> handleAddProduct(chunk);
                case 4 -> handleRemoveProduct(chunk);
                case 100 -> handlePurchase(chunk); // π.χ. αγορά
                default -> System.out.println("❌ Άγνωστο typeID: " + chunk.getTypeID());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void handleInsertStore(Chunk chunk) {
        Store store = (Store) chunk.getData();
        synchronized (storeList) {
            storeList.add(store);
            System.out.println("🏪 Κατάστημα '" + store.getStoreName() + "' αποθηκεύτηκε στον Worker.");
        }
    }

    private static void handleUpdateAvailability(Chunk chunk) {
        Map<String, Object> data = (Map<String, Object>) chunk.getData();
        String storeName = (String) data.get("storeName");
        String productName = (String) data.get("productName");
        int newAmount = (int) data.get("newAmount");

        synchronized (storeList) {
            for (Store store : storeList) {
                if (store.getStoreName().equalsIgnoreCase(storeName)) {
                    for (Product p : store.getProducts()) {
                        if (p.getProductName().equalsIgnoreCase(productName)) {
                            p.setAvailableAmount(newAmount);
                            System.out.println("🔄 Ενημερώθηκε ποσότητα '" + productName + "' στο κατάστημα '" + storeName + "' -> " + newAmount);
                            return;
                        }
                    }
                }
            }
        }
    }

    private static void handleAddProduct(Chunk chunk) {
        Map<String, Object> data = (Map<String, Object>) chunk.getData();
        String storeName = (String) data.get("storeName");
        Product newProduct = (Product) data.get("product");

        synchronized (storeList) {
            for (Store store : storeList) {
                if (store.getStoreName().equalsIgnoreCase(storeName)) {
                    store.getProducts().add(newProduct);
                    System.out.println("➕ Προστέθηκε νέο προϊόν '" + newProduct.getProductName() + "' στο κατάστημα '" + storeName + "'");
                    return;
                }
            }
        }

        System.out.println("❌ Δεν βρέθηκε κατάστημα για προσθήκη προϊόντος.");
    }

    private static void handleRemoveProduct(Chunk chunk) {
        Map<String, Object> data = (Map<String, Object>) chunk.getData();
        String storeName = (String) data.get("storeName");
        String productName = (String) data.get("productName");

        synchronized (storeList) {
            for (Store store : storeList) {
                if (store.getStoreName().equalsIgnoreCase(storeName)) {
                    boolean removed = store.getProducts().removeIf(p -> p.getProductName().equalsIgnoreCase(productName));
                    if (removed) {
                        System.out.println("❌ Το προϊόν '" + productName + "' αφαιρέθηκε από το κατάστημα '" + storeName + "'");
                    } else {
                        System.out.println("⚠ Το προϊόν δεν βρέθηκε στο κατάστημα.");
                    }
                    return;
                }
            }
        }

        System.out.println("❌ Δεν βρέθηκε κατάστημα για αφαίρεση προϊόντος.");
    }

    private static void handlePurchase(Chunk chunk) {
        // Not yet implemented: θα περιλαμβάνει ενημέρωση των αποθεμάτων, πωλήσεων, και συγχρονισμό.
    }
}
