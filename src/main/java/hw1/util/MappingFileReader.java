package hw1.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class MappingFileReader {

    private static final Pattern splitter = Pattern.compile(",");

    /**
     * Static method that reads mapping
     * @param configuration hadoop configuration class
     * @param path hadoop fs path
     * @return Map id -> name
     * @throws IOException when file can not be opened or file has incorrect internal structure
     */
    public static Map<Integer, String> read(Configuration configuration, Path path) throws IOException {
        Map<Integer, String> result = new HashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
        int device;
        String line;
        while ((line = reader.readLine()) != null){
            try {
                String [] data = splitter.split(line);
                device = Integer.parseInt(data[0]);
                result.put(device, data[1]);
            }
            catch (NumberFormatException noexcept) {
                throw new IOException("Wrong file format");
            }
        }
        return result;
    }
}
