package com.example.dsd20252.model;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * Κλάση ManagerConsole - Διαχειρίζεται τις λειτουργίες του manager.
 * Επιτρέπει την προσθήκη καταστημάτων, προϊόντων, την ενημέρωση αποθέματος
 * και την εμφάνιση συνολικών πωλήσεων ανά προϊόν, τύπο καταστήματος ή κατηγορία προϊόντος.
 */
public class ManagerConsole {
    private static String masterHost;
    private static int masterPort;

    public static void main(String[] args) {
        loadConfig();

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.println("\n=== Manager Console ===");
                System.out.println("1. Προσθήκη καταστήματος");
                System.out.println("2. Ενημέρωση διαθεσιμότητας προϊόντος");
                System.out.println("3. Προσθήκη νέου προϊόντος");
                System.out.println("4. Αφαίρεση προϊόντος");
                System.out.println("5. Συνολικές πωλήσεις ανά προϊόν");
                System.out.println("6. Συνολικές πωλήσεις ανά τύπο καταστήματος");
                System.out.println("7. Συνολικές πωλήσεις ανά κατηγορία προϊόντος");
                System.out.println("8. Έξοδος");
                System.out.print("Επιλογή: ");

                String choice = scanner.nextLine();

                switch (choice) {
                    case "1" -> insertStore(scanner);
                    case "2" -> updateProductAmount(scanner);
                    case "3" -> addNewProduct(scanner);
                    case "4" -> removeProduct(scanner);
                    case "5" -> requestSalesByProduct();
                    case "6" -> requestSalesByStoreType();
                    case "7" -> requestSalesByProductCategory();
                    case "8" -> System.exit(0);
                    default -> System.out.println("Μη έγκυρη επιλογή.");
                }
            }
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
    // Εισαγωγή καταστήματος
    private static void insertStore(Scanner scanner) {
        System.out.print("📁 Δώσε path φακέλου που περιέχει το store.json και logo: ");
        String folderPath = scanner.nextLine();
        try {
            Store store = StoreParser.parseStoreFromJson(folderPath);
            Chunk chunk = new Chunk("admin", 1, store);
            sendToMaster(chunk, true);
        } catch (Exception e) {
            System.out.println("Σφάλμα κατά την ανάγνωση JSON: " + e.getMessage());
        }
    }
    // Αποστολή ενημέρωσης αποθέματος για συγκεκριμένο προϊόν
    private static void updateProductAmount(Scanner scanner) {
        System.out.print("Όνομα καταστήματος: ");
        String storeName = scanner.nextLine();
        System.out.print("Όνομα προϊόντος: ");
        String productName = scanner.nextLine();
        System.out.print("Νέα ποσότητα διαθέσιμου προϊόντος: ");
        int newAmount = Integer.parseInt(scanner.nextLine());

        Map<String, Object> data = new HashMap<>();
        data.put("storeName", storeName);
        data.put("productName", productName);
        data.put("newAmount", newAmount);

        Chunk chunk = new Chunk("admin", 2, data);
        sendToMaster(chunk, true);
    }
    //Προσθήκη νέου προιοντος
    private static void addNewProduct(Scanner scanner) {
        Map<String, Object> data = new HashMap<>();
        System.out.print("Κατάστημα: ");
        data.put("storeName", scanner.nextLine());

        Product product = new Product();
        System.out.print("Όνομα προϊόντος: ");
        product.setProductName(scanner.nextLine());
        System.out.print("Τύπος προϊόντος: ");
        product.setProductType(scanner.nextLine());
        System.out.print("Διαθέσιμη ποσότητα: ");
        product.setAvailableAmount(Integer.parseInt(scanner.nextLine()));
        System.out.print("Τιμή: ");
        product.setPrice(Double.parseDouble(scanner.nextLine()));

        data.put("product", product);

        Chunk chunk = new Chunk("admin", 3, data);
        sendToMaster(chunk, true);
    }
    //Αφαίορεση Προιόντος
    private static void removeProduct(Scanner scanner) {
        Map<String, Object> data = new HashMap<>();
        System.out.print("Κατάστημα: ");
        data.put("storeName", scanner.nextLine());
        System.out.print("Όνομα προϊόντος για αφαίρεση: ");
        data.put("productName", scanner.nextLine());

        Chunk chunk = new Chunk("admin", 4, data);
        sendToMaster(chunk, true);
    }

    private static void requestSalesByProduct() {
        Chunk chunk = new Chunk("admin", 5, null);
        sendToMaster(chunk, true);
    }

    private static void requestSalesByStoreType() {
        Chunk chunk = new Chunk("admin", 6, null);
        sendToMaster(chunk, true);
    }

    private static void requestSalesByProductCategory() {
        Chunk chunk = new Chunk("admin", 7, null);
        sendToMaster(chunk, true);
    }

    //Αποστολή chunk στον Master
    private static void sendToMaster(Chunk chunk, boolean expectsResponse) {
        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(chunk);
            out.flush();
            System.out.println("Request sent to Master.");

            if (expectsResponse) {
                Chunk response = (Chunk) in.readObject();
                System.out.println("Response from Master:");
                System.out.println(response.getData());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
