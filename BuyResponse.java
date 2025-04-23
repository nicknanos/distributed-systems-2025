
import java.io.Serializable;

public class BuyResponse implements Serializable {
    private boolean success;
    private String message;

    public BuyResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }
}
