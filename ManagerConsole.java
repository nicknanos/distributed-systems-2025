
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ManagerConsole {

    private static String masterHost;
    private static int masterPort;

    public static void main(String[] args) {
        loadConfig();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.println("\n=== Manager Console ===");
                System.out.println("1. Insert store from JSON");
                System.out.println("2. Update product availability");
                System.out.println("3. Search store by name");
                System.out.println("4. View statistics");
                System.out.println("5. Exit");
                System.out.print("Choose: ");
                String choice = reader.readLine();

                switch (choice) {
                    case "1" -> insertStore(reader);
                    case "2" -> updateAvailability(reader);
                    case "3" -> searchStore(reader);
                    case "4" -> requestStatistics();
                    case "5" -> System.exit(0);
                    default -> System.out.println("Invalid choice.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadConfig() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("manager.config"));
            masterHost = prop.getProperty("host");
            masterPort = Integer.parseInt(prop.getProperty("masterPort"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void insertStore(BufferedReader reader) {
        try {
            System.out.print("Enter path to JSON file: ");
            String path = reader.readLine();
            String json = Files.readString(Paths.get(path));

            Store store = StoreParser.fromJson(json);
            Chunk chunk = new Chunk("admin", 1, store);

            sendToMaster(chunk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void updateAvailability(BufferedReader reader) {
        try {
            System.out.print("Enter store name: ");
            String storeName = reader.readLine();
            System.out.print("Enter product name: ");
            String productName = reader.readLine();
            System.out.print("Enter new available amount: ");
            int amount = Integer.parseInt(reader.readLine());

            Map<String, Object> update = new HashMap<>();
            update.put("storeName", storeName);
            update.put("productName", productName);
            update.put("newAmount", amount);

            Chunk chunk = new Chunk("admin", 4, update); // typeID 4 = update amount
            sendToMaster(chunk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void searchStore(BufferedReader reader) {
        try {
            System.out.print("Enter store name to search: ");
            String storeName = reader.readLine();
            Chunk chunk = new Chunk("admin", 5, storeName); // typeID 5 = search by name

            sendToMaster(chunk);
            //receiveFromMaster();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void requestStatistics() {
        Chunk chunk = new Chunk("admin", 6, null); // typeID 6 = request stats
        sendToMaster(chunk);
        //receiveFromMaster();
    }

    private static void sendToMaster(Chunk chunk) {
        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(chunk);
            out.flush();
            System.out.println("✅ Request sent to Master.");

            // Receive response (for typeID 4, 5, 6)
            Chunk response = (Chunk) in.readObject();
            System.out.println("✅ Response from Master:");
            System.out.println(response.getData());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void receiveFromMaster() {
        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Chunk response = (Chunk) in.readObject();
            System.out.println("✅ Response from Master:");
            System.out.println(response.getData());
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
