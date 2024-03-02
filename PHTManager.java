import java.util.Arrays;
import java.util.List;

class PHTManager {
    public static void main(String[] args) {
        List<Integer> tempInput = Arrays.asList(3, 4, 5);
        Hash_Table test = new Hash_Table(tempInput, "temp");
        test.partitionedHash(Arrays.asList("red", "apple", "sunday"));
    }
}
