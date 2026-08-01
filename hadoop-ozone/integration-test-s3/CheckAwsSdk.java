import com.amazonaws.services.s3.model.PutObjectRequest;
import java.lang.reflect.Method;

public class CheckAwsSdk {
    public static void main(String[] args) {
        Method[] methods = PutObjectRequest.class.getMethods();
        for (Method m : methods) {
            if (m.getName().toLowerCase().contains("objectlock")) {
                System.out.println(m.getName());
            }
        }
    }
}
