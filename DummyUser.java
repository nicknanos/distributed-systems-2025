import java.io.*;
import java.net.Socket;
import java.util.*;

public class DummyUser {

    private static String masterHost;
    private static int masterPort;

    public static void main(String[] args) {
        loadConfig();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== Dummy User Menu ===");
            System.out.println("1. Αναζήτηση καταστημάτων");
            System.out.println("2. Έξοδος");
            System.out.print("Επιλογή: ");
            String choice = scanner.nextLine();

            switch (choice) {
                case "1" -> search(scanner);
                case "2" -> System.exit(0);
                default -> System.out.println("Μη έγκυρη επιλογή.");
            }
        }
    }

    private static void loadConfig() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("user.config"));
            masterHost = prop.getProperty("host");
            masterPort = Integer.parseInt(prop.getProperty("masterPort"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void search(Scanner scanner) {
        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Διαβάζουμε τα φίλτρα από τον χρήστη
            Map<String, Object> filters = new HashMap<>();

            System.out.print("📍 Latitude: ");
            filters.put("latitude", Double.parseDouble(scanner.nextLine()));

            System.out.print("📍 Longitude: ");
            filters.put("longitude", Double.parseDouble(scanner.nextLine()));

            System.out.print("🍽 Food Category (π.χ. pizzeria ή αφήστε κενό): ");
            String food = scanner.nextLine();
            if (!food.isEmpty()) filters.put("foodCategory", food);

            System.out.print("⭐ Ελάχιστα αστέρια (1-5 ή κενό): ");
            String stars = scanner.nextLine();
            if (!stars.isEmpty()) filters.put("stars", Integer.parseInt(stars));

            System.out.print("💲 Price Category ($/$$/$$$ ή κενό): ");
            String price = scanner.nextLine();
            if (!price.isEmpty()) filters.put("priceCategory", price);

            // Δημιουργούμε ένα Chunk για αναζήτηση (typeID 10 π.χ.)
            Chunk searchRequest = new Chunk("dummyuser", 10, filters);

            // Στέλνουμε το αίτημα στο Master
            out.writeObject(searchRequest);
            out.flush();
            System.out.println("✅ Search request sent to Master.");

            // Περιμένουμε την απάντηση
            Chunk response = (Chunk) in.readObject();
            List<Store> foundStores = (List<Store>) response.getData();

            if (foundStores.isEmpty()) {
                System.out.println("❌ Δεν βρέθηκαν καταστήματα με αυτά τα φίλτρα.");
            } else {
                System.out.println("\n🔎 Βρέθηκαν καταστήματα:");
                for (Store s : foundStores) {
                    System.out.println("🏪 " + s.getStoreName() + " | " + s.getFoodCategory() + " | " + s.getStars() + "⭐ | " + s.getPriceCategory());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
