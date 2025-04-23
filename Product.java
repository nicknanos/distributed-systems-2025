import java.io.Serializable;

public class Product implements Serializable {
    private String productName;
    private String productType;
    private int availableAmount;
    private double price;

    public String getProductName() { return productName; }
    public String getProductType() { return productType; }
    public int getAvailableAmount() { return availableAmount; }
    public double getPrice() { return price; }

    public void setProductName(String productName) { this.productName = productName; }
    public void setProductType(String productType) { this.productType = productType; }
    public void setAvailableAmount(int availableAmount) { this.availableAmount = availableAmount; }
    public void setPrice(double price) { this.price = price; }
}