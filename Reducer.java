
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Reducer {
    private static int serverPort;
    private static String masterHost;
    private static int masterPort;
    private static int expectedChunks;

    private static final Map<Integer, ArrayList<Chunk>> chunkMap = new HashMap<>();

    public static void init() {
        Properties prop = new Properties();
        String filename = "reducer.config";

        try (FileInputStream f = new FileInputStream(filename)) {
            prop.load(f);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        masterHost = prop.getProperty("masterHost");
        masterPort = Integer.parseInt(prop.getProperty("masterPort"));
        serverPort = Integer.parseInt(prop.getProperty("serverPort"));
        expectedChunks = Integer.parseInt(prop.getProperty("expectedChunks"));
    }

    public static void main(String[] args) {
        init();
        startReducerServer();
    }

    private static void startReducerServer() {
        try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
            while (true) {
                Socket workerSocket = serverSocket.accept();
                new Thread(() -> handleWorkerRequest(workerSocket)).start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void handleWorkerRequest(Socket workerSocket) {
        try (ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {
            while (true) {
                Chunk chunk = (Chunk) in.readObject();
                if (chunk.getTypeID() == 2) {
                    synchronized (chunkMap) {
                        int chunkId = chunk.getSegmentID();
                        chunkMap.putIfAbsent(chunkId, new ArrayList<>());
                        chunkMap.get(chunkId).add(chunk);

                        if (chunkMap.get(chunkId).size() == expectedChunks) {
                            Chunk merged = mergeStores(chunkMap.get(chunkId));
                            sendToMaster(merged);
                            chunkMap.remove(chunkId);
                        }
                    }
                } else {
                    sendToMaster(chunk);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void sendToMaster(Chunk chunk) {
        try (Socket masterSocket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(masterSocket.getOutputStream())) {

            out.writeObject(chunk);
            out.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Chunk mergeStores(ArrayList<Chunk> chunks) {
        String userID = chunks.get(0).getUserID();
        int segmentID = chunks.get(0).getSegmentID();
        int typeID = chunks.get(0).getTypeID();

        List<Store> mergedList = new ArrayList<>();

        for (Chunk chunk : chunks) {
            List<Store> storesFromWorker = (List<Store>) chunk.getData();
            mergedList.addAll(storesFromWorker);
        }

        Chunk finalChunk = new Chunk(userID, typeID, mergedList);
        finalChunk.setSegmentID(segmentID);
        return finalChunk;
    }
}
