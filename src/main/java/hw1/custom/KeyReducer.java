package hw1.custom;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom key for reducer
 */
@AllArgsConstructor
@NoArgsConstructor
public class KeyReducer implements WritableComparable<KeyReducer> {

    /**
     * Device name
     */
    @Getter
    @Setter
    private String device;

    /**
     * Truncated timestamp according to aggregation interval
     */
    @Getter
    @Setter
    private long timestamp;

    /**
     * String representation of aggregation interval
     */
    @Getter
    @Setter
    private String interval;

    /**
     * Implementation of write() method
     * @param dataOutput data output to write in
     * @throws IOException when write() fails
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(device);
        dataOutput.writeLong(timestamp);
        dataOutput.writeUTF(interval);
    }

    /**
     * Implementation of readFields() method
     * @param dataInput data input to read from
     * @throws IOException when read() fails
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        device = dataInput.readUTF();
        timestamp = dataInput.readLong();
        interval = dataInput.readUTF();
    }

    /**
     * Implementation of compareTo() method.
     * Compares keys in the following order: 1->id, 2->timestamp
     * @param keyReducer the second object
     * @return -1, 0, 1 that means less, equal or greater respectively
     */
    @Override
    public int compareTo(KeyReducer keyReducer) {
        if (device.equals(keyReducer.device))
            return Long.compare(timestamp, keyReducer.timestamp);
        return device.compareTo(keyReducer.device);
    }

    /**
     * Implementation of Object.equals() method
     * @param o the second object
     * @return true if equals else false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        KeyReducer that = (KeyReducer) o;

        if (timestamp != that.timestamp)
            return false;
        if (!device.equals(that.device))
            return false;
        return interval.equals(that.interval);
    }

    /**
     * Implementation of Object.hashCode() method
     * @return hash code
     */
    @Override
    public int hashCode() {
        int result = device.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + interval.hashCode();
        return result;
    }

    /**
     * Implementation of Object.toString() method
     * @return string representation
     */
    @Override
    public String toString() {
        return "KeyReducer{ id=" + device +
                ", timestamp=" + timestamp +
                ", interval='" + interval + "'}";
    }
}
