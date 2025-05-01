
import java.io.*;
import java.net.Socket;
import java.util.*;

public class ActionsForWorkers extends Thread {
    private final Chunk data;
    private final Map<String, Store> stores;
    private final Socket masterSocket;

    public ActionsForWorkers(Chunk data, Map<String, Store> stores, Socket masterSocket) {
        this.data = data;
        this.stores = stores;
        this.masterSocket = masterSocket;
    }

    @Override
    public void run() {
        switch (data.getTypeID()) {
            case 2 -> handleSearch();
            //case 3 -> handleBuy();
            default -> System.out.println("Invalid request type in Worker.");
        }
    }

    private void handleSearch() {
        SearchRequest request = (SearchRequest) data.getData();
        List<Store> matched = new ArrayList<>();

        synchronized (stores) {
            for (Store store : stores.values()) {
                if (storeMatches(store, request)) {
                    matched.add(store);
                }
            }
        }

        try {
            ObjectOutputStream out = new ObjectOutputStream(masterSocket.getOutputStream());
            Chunk response = new Chunk(data.getUserID(), data.getTypeID(), matched);
            response.setSegmentID(data.getSegmentID());
            out.writeObject(response);
            out.flush();
            System.out.println("Sent search result back to Master: " + matched.size() + " stores.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean storeMatches(Store store, SearchRequest req) {
        double dist = haversine(req.getLatitude(), req.getLongitude(), store.getLatitude(), store.getLongitude());
        if (dist > 5.0) return false;
        if (!req.getCategories().isEmpty() && !req.getCategories().contains(store.getFoodCategory())) return false;
        if (store.getStars() < req.getMinStars()) return false;
        if (!req.getPriceCategories().isEmpty() && !req.getPriceCategories().contains(store.getPriceCategory())) return false;
        return true;
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
/*
    private void handleBuy() {
        BuyRequest req = (BuyRequest) data.getData();
        BuyResponse response;

        synchronized (stores) {
            Store store = stores.get(req.getStoreName());
            if (store == null) {
                response = new BuyResponse(false, "Store not found.");
            } else {
                boolean available = true;
                for (Map.Entry<String, Integer> entry : req.getProductQuantities().entrySet()) {
                    String productName = entry.getKey();
                    int quantity = entry.getValue();
                    Product product = store.getProducts().stream()
                        .filter(p -> p.getProductName().equals(productName))
                        .findFirst()
                        .orElse(null);
                    if (product == null || product.getAvailableAmount() < quantity) {
                        available = false;
                        break;
                    }
                }

                if (available) {
                    for (Map.Entry<String, Integer> entry : req.getProductQuantities().entrySet()) {
                        String productName = entry.getKey();
                        int quantity = entry.getValue();
                        store.getProducts().forEach(p -> {
                            if (p.getProductName().equals(productName)) {
                                p.setAvailableAmount(p.getAvailableAmount() - quantity);
                            }
                        });
                    }
                    response = new BuyResponse(true, "Purchase successful.");
                } else {
                    response = new BuyResponse(false, "Not enough stock.");
                }
            }
        }

        try {
            ObjectOutputStream out = new ObjectOutputStream(masterSocket.getOutputStream());
            Chunk responseChunk = new Chunk(data.getUserID(), 3, response);
            responseChunk.setSegmentID(data.getSegmentID());
            out.writeObject(responseChunk);
            out.flush();
            System.out.println("Sent buy result to Master: " + response.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
*/
}
