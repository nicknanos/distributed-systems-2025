import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * Η κλάση DummyUser προσομοιώνει τη συμπεριφορά ενός πελάτη που συνδέεται με τον Master,
 * εκτελεί αναζητήσεις με βάση φίλτρα, αγοράζει προϊόντα και βαθμολογεί καταστήματα μέσω TCP sockets.
 */
public class DummyUser {

    private static String masterHost;
    private static int masterPort;

    public static void main(String[] args) {
        loadConfig();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== Dummy User Menu ===");
            System.out.println("1. Αναζήτηση καταστημάτων");
            System.out.println("2. Αγορά Προϊόντος");
            System.out.println("3. Βαθμολόγηση καταστήματος");
            System.out.println("4. Έξοδος");
            System.out.print("Επιλογή: ");
            String choice = scanner.nextLine();

            switch (choice) {
                case "1" -> search(scanner);
                case "2" -> buy(scanner);
                case "3" -> handleRating(scanner);
                case "4" -> System.exit(0);
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

    /**
     * Αποστέλλει φίλτρα στο Master και λαμβάνει λίστα καταστημάτων που τα ικανοποιούν.
     */
    private static void search(Scanner scanner) {
        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Διαβάζουμε τα φίλτρα από τον χρήστη
            Map<String, Object> filters = new HashMap<>();

            System.out.print("Latitude: ");
            filters.put("latitude", Double.parseDouble(scanner.nextLine()));

            System.out.print("Longitude: ");
            filters.put("longitude", Double.parseDouble(scanner.nextLine()));

            System.out.print("🍽 Food Category (π.χ. pizzeria): ");
            String food = scanner.nextLine();
            if (!food.isEmpty()) filters.put("foodCategory", food);

            System.out.print("Ελάχιστα αστέρια (1-5): ");
            String stars = scanner.nextLine();
            if (!stars.isEmpty()) filters.put("stars", Integer.parseInt(stars));

            System.out.print("Price Category ($/$$/$$$): ");
            String price = scanner.nextLine();
            if (!price.isEmpty()) filters.put("priceCategory", price);

            // Δημιουργία και αποστολή Chunk αναζήτησης
            Chunk searchRequest = new Chunk("dummyuser", 10, filters);

            out.writeObject(searchRequest);
            out.flush();
            System.out.println("Search request sent to Master.");

            // Λήψη και εμφάνιση αποτελεσμάτων
            Chunk response = (Chunk) in.readObject();
            List<Store> foundStores = (List<Store>) response.getData();

            if (foundStores.isEmpty()) {
                System.out.println("Δεν βρέθηκαν καταστήματα με αυτά τα φίλτρα.");
            } else {
                System.out.println("\nΒρέθηκαν καταστήματα:");
                for (Store s : foundStores) {
                    System.out.println(s.getStoreName() + " | " + s.getFoodCategory() + " | " + s.getStars() + "| " + s.getPriceCategory());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // Αποστέλλει αίτημα αγοράς στο Master.
    private static void buy(Scanner in) {


        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream input = new ObjectInputStream(socket.getInputStream())) {
                System.out.print("Διάλεξε κατάστημα: ");
                String storeName = in.nextLine();

                System.out.print("Προϊόν προς αγορά: ");
                String productName = in.nextLine();

                System.out.print("Ποσότητα: ");
                int quantity = Integer.parseInt(in.nextLine());

                // Δημιουργία και αποστολή Chunk αγοράς
                BuyRequest buyRequest = new BuyRequest(storeName, productName, quantity);
                Chunk chunk = new Chunk("user", 11, buyRequest);
                out.writeObject(chunk);
                out.flush();

                // Εμφάνιση αποτελέσματος
                Chunk response = (Chunk) input.readObject();
                System.out.println("Αποτέλεσμα αγοράς: " + response.getData());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    //Αποστέλλει νέα βαθμολογία καταστήματος στο Master.
    private static void handleRating(Scanner in) {
        System.out.print("Όνομα καταστήματος: ");
        String storeName = in.nextLine();

        System.out.print("Αστέρια (1-5): ");
        int rating = Integer.parseInt(in.nextLine());

        Map<String, Object> data = new HashMap<>();
        data.put("storeName", storeName);
        data.put("rating", rating);

        Chunk chunk = new Chunk("user", 12, data);

        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream input = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(chunk);
            out.flush();

            Chunk response = (Chunk) input.readObject();
            System.out.println("Απάντηση: " + response.getData());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }



}
