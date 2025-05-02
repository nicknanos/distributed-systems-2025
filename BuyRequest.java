import java.io.Serializable;

//Helper Class for the purchase function
public class BuyRequest implements Serializable {
    private String storeName;
    private String productName;
    private int quantity;

    public BuyRequest(String storeName, String productName, int quantity) {
        this.storeName = storeName;
        this.productName = productName;
        this.quantity = quantity;
    }

    public String getStoreName() {
        return storeName;
    }

    public String getProductName() {
        return productName;
    }

    public int getQuantity() {
        return quantity;
    }
}
