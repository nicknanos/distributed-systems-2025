
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ManagerConsole {

    private static String masterHost;
    private static int masterPort;

    public static void main(String[] args) {
        loadConfig();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.println("=== Manager Console ===");
                System.out.println("1. Insert store from JSON");
                System.out.println("2. Exit");
                System.out.print("Choose: ");
                String choice = reader.readLine();

                switch (choice) {
                    case "1" -> insertStore(reader);
                    case "2" -> System.exit(0);
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

            Store store = StoreParser.fromJson(json); // απαιτεί StoreParser
            Chunk chunk = new Chunk("admin", 1, store);

            try (Socket socket = new Socket(masterHost, masterPort);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                out.writeObject(chunk);
                out.flush();
                System.out.println("✅ Store sent to Master.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("❌ Failed to send store.");
        }
    }
}
