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
                case 5 -> handleSalesByProduct(chunk, socket);
                case 6 -> handleSalesByStoreType(socket);
                case 7 -> handleSalesByProductCategory(socket);
                case 10 -> handleSearchRequest(chunk);
                case 11 -> handleBuyRequest(chunk);
                case 12 -> handleRating(chunk);
                //case 100 -> handlePurchase(chunk, socket);
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

    private static void handleSalesByProduct(Chunk chunk, Socket socket) {
        Map<String, Integer> productSales = new HashMap<>();

        synchronized (storeList) {
            for (Store store : storeList) {
                for (Product product : store.getProducts()) {
                    // Αν έχει γίνει κάποια πώληση
                    if (product.getSoldAmount() > 0) {
                        productSales.put(product.getProductName(),
                                productSales.getOrDefault(product.getProductName(), 0) + product.getSoldAmount());
                    }
                }
            }
        }

        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            Chunk response = new Chunk("worker", 5, productSales);
            response.setSegmentID(chunk.getSegmentID());
            out.writeObject(response);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void handleSalesByStoreType(Socket socket) {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            Map<String, Integer> storeTypeSales = new HashMap<>();

            synchronized (storeList) {
                for (Store store : storeList) {
                    int storeSales = 0;
                    for (Product p : store.getProducts()) {
                        int initialAmount = p.getInitialAmount();
                        int currentAmount = p.getAvailableAmount();
                        storeSales += (initialAmount - currentAmount);
                    }

                    if (storeSales > 0) {
                        storeTypeSales.merge(store.getFoodCategory(), storeSales, Integer::sum);
                    }
                }
            }

            Chunk response = new Chunk("worker", 6, storeTypeSales);
            out.writeObject(response);
            out.flush();
            System.out.println("📦 Sales per store type sent back to Master!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void handleSalesByProductCategory(Socket socket) {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            Map<String, Integer> productCategorySales = new HashMap<>();

            synchronized (storeList) {
                for (Store store : storeList) {
                    for (Product p : store.getProducts()) {
                        int initialAmount = p.getInitialAmount();
                        int currentAmount = p.getAvailableAmount();
                        int sold = initialAmount - currentAmount;

                        if (sold > 0) {
                            productCategorySales.merge(p.getProductType(), sold, Integer::sum);
                        }
                    }
                }
            }

            Chunk response = new Chunk("worker", 7, productCategorySales);
            out.writeObject(response);
            out.flush();
            System.out.println("📦 Sales per product category sent back to Master!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleSearchRequest(Chunk chunk) {
        try {
            Map<String, Object> filters = (Map<String, Object>) chunk.getData();
            double clientLat = (double) filters.get("latitude");
            double clientLon = (double) filters.get("longitude");
            String foodCategory = (String) filters.get("foodCategory");
            int minStars = (int) filters.get("stars");
            String priceCategory = (String) filters.get("priceCategory");

            List<Store> results = new ArrayList<>();

            synchronized (storeList) {
                for (Store store : storeList) {
                    if (distance(clientLat, clientLon, store.getLatitude(), store.getLongitude()) <= 5) {
                        if (!foodCategory.isEmpty() && !store.getFoodCategory().equalsIgnoreCase(foodCategory)) {
                            continue;
                        }
                        if (store.getStars() < minStars) {
                            continue;
                        }
                        if (!priceCategory.isEmpty() && !store.getPriceCategory().equals(priceCategory)) {
                            continue;
                        }
                        results.add(store);
                    }
                }
            }

            // Στέλνουμε τα αποτελέσματα πίσω στον Master
            sendResultsToMaster(chunk.getSegmentID(), results);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleBuyRequest(Chunk chunk) {
        BuyRequest req = (BuyRequest) chunk.getData();
        String storeName = req.getStoreName();
        String productName = req.getProductName();
        int quantity = req.getQuantity();

        synchronized (storeList) {
            for (Store store : storeList) {
                if (store.getStoreName().equalsIgnoreCase(storeName)) {
                    for (Product p : store.getProducts()) {
                        if (p.getProductName().equalsIgnoreCase(productName)) {
                            synchronized (p) {
                                int available = p.getAvailableAmount();
                                if (available >= quantity) {
                                    p.setAvailableAmount(available - quantity);
                                    System.out.println("✅ Αγορά: " + quantity + " x " + productName + " από " + storeName);
                                    return;
                                } else {
                                    System.out.println("❌ Μη διαθέσιμο απόθεμα για " + productName + " στο " + storeName);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("❌ Κατάστημα ή προϊόν δεν βρέθηκαν για αγορά.");
    }


    private static void handleRating(Chunk chunk) {
        Map<String, Object> data = (Map<String, Object>) chunk.getData();
        String storeName = (String) data.get("storeName");
        int rating = (int) data.get("rating");

        synchronized (storeList) {
            for (Store store : storeList) {
                if (store.getStoreName().equalsIgnoreCase(storeName)) {
                    int oldStars = store.getStars();
                    int oldVotes = store.getNoOfVotes();
                    int newVotes = oldVotes + 1;
                    int newStars = Math.round(((oldStars * oldVotes) + rating) / (float) newVotes);
                    store.setStars(newStars);
                    store.setNoOfVotes(newVotes);
                    System.out.println("⭐ Νέα βαθμολογία για '" + storeName + "': " + newStars + " (" + newVotes + " ψήφοι)");
                    return;
                }
            }
        }
    }



    private static void sendResultsToMaster(int segmentId, List<Store> results) {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("worker.config"));
            String masterHost = prop.getProperty("masterHost");
            int masterPort = Integer.parseInt(prop.getProperty("reducerPort"));

            try (Socket socket = new Socket(masterHost, masterPort);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                Chunk response = new Chunk("worker", 10, results);
                response.setSegmentID(segmentId);

                out.writeObject(response);
                out.flush();
                System.out.println("✅ Στάλθηκαν αποτελέσματα αναζήτησης στον Master (" + results.size() + " καταστήματα).");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
        dist = Math.acos(dist);
        dist = Math.toDegrees(dist);
        dist = dist * 60 * 1.1515 * 1.609344; // miles to km
        System.out.println(dist);
        return dist;

    }

}


