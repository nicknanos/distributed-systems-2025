
import java.io.Serializable;
import java.util.List;

public class SearchRequest implements Serializable {
    private double latitude;
    private double longitude;
    private List<String> categories;
    private int minStars;
    private List<String> priceCategories;

    public SearchRequest(double latitude, double longitude, List<String> categories, int minStars, List<String> priceCategories) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.categories = categories;
        this.minStars = minStars;
        this.priceCategories = priceCategories;
    }

    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public List<String> getCategories() { return categories; }
    public int getMinStars() { return minStars; }
    public List<String> getPriceCategories() { return priceCategories; }
}
