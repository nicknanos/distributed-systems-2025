import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StoreParser {

    public static Store parseStoreFromJson(String folderPath) throws Exception {
        File jsonFile = new File(folderPath, "store.json");
        FileInputStream fis = new FileInputStream(jsonFile);
        byte[] data = fis.readAllBytes();
        fis.close();
        String json = new String(data, StandardCharsets.UTF_8);

        JSONObject obj = new JSONObject(json);
        Store store = new Store();

        store.setStoreName(obj.getString("StoreName"));
        store.setLatitude(obj.getDouble("Latitude"));
        store.setLongitude(obj.getDouble("Longitude"));
        store.setFoodCategory(obj.getString("FoodCategory"));
        store.setStars(obj.getInt("Stars"));
        store.setNoOfVotes(obj.getInt("NoOfVotes"));
        store.setStoreLogo(obj.getString("StoreLogo"));

        JSONArray productsJson = obj.getJSONArray("Products");
        List<Product> products = new ArrayList<>();
        double sum = 0;

        for (int i = 0; i < productsJson.length(); i++) {
            JSONObject p = productsJson.getJSONObject(i);
            Product product = new Product();
            product.setProductName(p.getString("ProductName"));
            product.setProductType(p.getString("ProductType"));
            product.setAvailableAmount(p.getInt("Available Amount"));
            product.setPrice(p.getDouble("Price"));
            sum += product.getPrice();
            products.add(product);
        }

        store.setProducts(products);

        double avg = sum / products.size();
        if (avg <= 5) store.setPriceCategory("$");
        else if (avg <= 15) store.setPriceCategory("$$");
        else store.setPriceCategory("$$$");
        return store;
    }
}
