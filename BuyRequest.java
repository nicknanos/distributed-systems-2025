
import java.io.Serializable;
import java.util.Map;

public class BuyRequest implements Serializable {
    private String storeName;
    private Map<String, Integer> productQuantities;

    public BuyRequest(String storeName, Map<String, Integer> productQuantities) {
        this.storeName = storeName;
        this.productQuantities = productQuantities;
    }

    public String getStoreName() {
        return storeName;
    }

    public Map<String, Integer> getProductQuantities() {
        return productQuantities;
    }
}
