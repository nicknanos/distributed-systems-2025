import java.io.*;
import java.net.Socket;
import java.util.*;

public class DummyUser {

    private static final Scanner scanner = new Scanner(System.in);
    private static String masterHost;
    private static int masterPort;
    private static int userId = 1; // fixed ID for simplicity

    public static void main(String[] args) {
        loadConfig();
        while (true) {
            printMenu();
            int choice = Integer.parseInt(scanner.nextLine());

            switch (choice) {
                case 1 -> handleSearch();
                case 2 -> handleBuy();
                case 3 -> System.exit(0);
                default -> System.out.println("Invalid choice.");
            }
        }
    }

    private static void loadConfig() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("user.config")); // config with masterHost, masterPort
            masterHost = prop.getProperty("host");
            masterPort = Integer.parseInt(prop.getProperty("masterPort"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printMenu() {
        System.out.println("\n--- Dummy User Menu ---");
        System.out.println("1. Search stores");
        System.out.println("2. Buy from store");
        System.out.println("3. Exit");
        System.out.print("Your choice: ");
    }

    private static void handleSearch() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            System.out.println("Enter location (lat lon): ");
            double lat = scanner.nextDouble();
            double lon = scanner.nextDouble();
            scanner.nextLine(); // consume newline

            System.out.println("Enter categories (comma-separated) or leave blank: ");
            List<String> categories = List.of(scanner.nextLine().split(","));

            System.out.println("Minimum stars (0-5): ");
            int minStars = Integer.parseInt(scanner.nextLine());

            System.out.println("Price categories ($,$$,...) comma-separated: ");
            List<String> prices = List.of(scanner.nextLine().split(","));

            SearchRequest request = new SearchRequest(lat, lon, categories, minStars, prices);
            Chunk chunk = new Chunk(String.valueOf(userId), 2, request);

            out.writeObject(chunk);
            out.flush();

            Chunk response = (Chunk) in.readObject();
            List<Store> results = (List<Store>) response.getData();

            if (results.isEmpty()) {
                System.out.println("No stores found.");
            } else {
                System.out.println("\n--- Matching Stores ---");
                for (int i = 0; i < results.size(); i++) {
                    Store s = results.get(i);
                    System.out.println((i + 1) + ". " + s.getStoreName() + " | Category: " + s.getFoodCategory() + " | Rating: " + s.getStars());
                }
                storedSearchResults = results;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Store> storedSearchResults = new ArrayList<>();

    private static void handleBuy() {
        if (storedSearchResults.isEmpty()) {
            System.out.println("Please search first.");
            return;
        }

        try (Socket socket = new Socket(masterHost, masterPort)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            System.out.println("Select store by number: ");
            int storeIndex = Integer.parseInt(scanner.nextLine()) - 1;

            Store selected = storedSearchResults.get(storeIndex);
            Map<String, Integer> cart = new HashMap<>();

            System.out.println("Available products:");
            for (Product p : selected.getProducts()) {
                System.out.println("- " + p.getProductName() + " (" + p.getAvailableAmount() + " available) $" + p.getPrice());
            }

            while (true) {
                System.out.print("Enter product name to buy (or 'done'): ");
                String product = scanner.nextLine();
                if (product.equalsIgnoreCase("done")) break;

                System.out.print("Quantity: ");
                int qty = Integer.parseInt(scanner.nextLine());
                cart.put(product, qty);
            }

            BuyRequest buyRequest = new BuyRequest(selected.getStoreName(), cart);
            Chunk chunk = new Chunk(String.valueOf(userId), 3, buyRequest);

            out.writeObject(chunk);
            out.flush();

            Chunk response = (Chunk) in.readObject();
            BuyResponse result = (BuyResponse) response.getData();
            System.out.println(result.isSuccess() ? "✅ " : "❌ " + result.getMessage());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
