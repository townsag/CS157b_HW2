import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class Util {
    public static int partitionedHash(List<String> attributes, List<Integer> columnHashRanges)
            throws IllegalArgumentException {
        if (attributes.size() != columnHashRanges.size()) {
            throw new IllegalArgumentException("the number of attributes sent to the partitioned hash" +
                    "function does not match the number of attributes in the table");
        }

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");

            int runningHashValue = 0b00;
            for (int i = 0; i < columnHashRanges.size(); i++) {
                md.update(attributes.get(i).getBytes());
                int hashResult = bytesToInt(md.digest());
                int bitmask = (1 << columnHashRanges.get(i).intValue()) - 1;
                // System.out.println("i: " + Integer.toString(i) + ", running hash: " +
                // Integer.toBinaryString(runningHashValue));
                // System.out.println("md5 hash result: " + Integer.toBinaryString(hashResult));
                // System.out.println("bitmask: " + Integer.toBinaryString(bitmask));
                runningHashValue <<= columnHashRanges.get(i).intValue();
                // System.out.println("running hash After shift: " +
                // Integer.toBinaryString(runningHashValue));
                runningHashValue |= (hashResult & bitmask);
                // System.out.println("running hash After bitwise or: " +
                // Integer.toBinaryString(runningHashValue) + "\n");
                md.reset();
            }
            return runningHashValue;
        } catch (NoSuchAlgorithmException e) {
            System.out.println("error in partitionedHash function");
            e.printStackTrace();
            System.exit(1);
        }
        return -1;
    }

    private static int bytesToInt(byte[] bytes) {
        int result = 0;
        // the math.min ensures that the byte array fits in a normal java int
        for (int i = 0; i < Math.min(bytes.length, 4); i++) {
            // 8 bits in a byte
            result <<= 8;
            // the |= is like doing addition, the lowest 8 bts of the int result are all 0
            // so
            // we are or-ing them with the bits of this byte
            // the (bytes[i] & 0xff) is a bitwise and that ensures we only get the 8 least
            // significant bits
            // of the byte, dont know if its necessary
            result |= (bytes[i] & 0xff);
        }
        return result;
    }
}
