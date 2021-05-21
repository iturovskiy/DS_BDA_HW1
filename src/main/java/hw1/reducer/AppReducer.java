package hw1.reducer;

import hw1.custom.KeyMapper;
import hw1.custom.KeyReducer;
import hw1.util.MappingFileReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.Map;


public class AppReducer extends Reducer<KeyMapper, FloatWritable, KeyReducer, FloatWritable> {
    /**
     * Map device to device_name
     */
    private Map<Integer, String> mapping;

    /**
     * Initial reducer setup
     * @param context reducer context
     * @throws IOException in MappingReader.read()
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI[] paths = context.getCacheFiles();
        for(URI u : paths) {
            if(u.getPath().toLowerCase().equals("mapping")) {
                mapping = MappingFileReader.read(context.getConfiguration(), new Path(u.getPath()));
                break;
            }
        }
    }

    /**
     * Reduce function. Calculates average value in given interval
     * @param key key
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(KeyMapper key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException
    {
        float sum = 0.0F;
        int num = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            num += 1;
        }
        if (context == null)
            System.out.println("huy");
        context.write(new KeyReducer(mapping.get(key.getDevice()), key.getTimestamp(),
                        key.getInterval()), new FloatWritable(sum/num));
    }
}
