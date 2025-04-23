import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class StoreParser {

    public static Store fromJson(String json) {
        JSONObject obj = new JSONObject(json);

        Store store = new Store();

        store.setStoreName(obj.getString("storeName"));
        store.setLatitude(obj.getDouble("latitude"));
        store.setLongitude(obj.getDouble("longitude"));
        store.setFoodCategory(obj.getString("foodCategory"));
        store.setStars(obj.getInt("stars"));
        store.setNoOfVotes(obj.getInt("noOfVotes"));
       // store.setPriceCategory(obj.getString("priceCategory"));
        store.setStoreLogo(obj.getString("storeLogo"));

        JSONArray productArray = obj.getJSONArray("products");
        List<Product> products = new ArrayList<>();

        for (int i = 0; i < productArray.length(); i++) {
            JSONObject p = productArray.getJSONObject(i);
            Product prod = new Product();

            prod.setProductName(p.getString("productName"));
            prod.setProductType(p.getString("productType"));
            prod.setAvailableAmount(p.getInt("availableAmount"));
            prod.setPrice(p.getDouble("price"));

            products.add(prod);
        }

        store.setProducts(products);
        return store;
    }
}