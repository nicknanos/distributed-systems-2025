import java.io.Serializable;
import java.util.List;

public class Store implements Serializable {
    private String storeName;
    private double latitude;
    private double longitude;
    private String foodCategory;
    private int stars;
    private int noOfVotes;
    private String priceCategory;
    private String storeLogo;
    private List<Product> products;

    public String getStoreName() { return storeName; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public String getFoodCategory() { return foodCategory; }
    public int getStars() { return stars; }
    public int getNoOfVotes() { return noOfVotes; }
    public String getPriceCategory() { return priceCategory; }
    public String getStoreLogo() { return storeLogo; }
    public List<Product> getProducts() { return products; }

    public void setStoreName(String storeName) { this.storeName = storeName; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setFoodCategory(String foodCategory) { this.foodCategory = foodCategory; }
    public void setStars(int stars) { this.stars = stars; }
    public void setNoOfVotes(int noOfVotes) { this.noOfVotes = noOfVotes; }
    public void setPriceCategory(String priceCategory) { this.priceCategory = priceCategory; }
    public void setStoreLogo(String storeLogo) { this.storeLogo = storeLogo; }
    public void setProducts(List<Product> products) { this.products = products; }
}