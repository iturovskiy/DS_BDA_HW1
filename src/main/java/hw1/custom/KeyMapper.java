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
 * Custom key for mapper
 */
@AllArgsConstructor
@NoArgsConstructor
public class KeyMapper implements WritableComparable<KeyMapper> {

    /**
     * Device id
     */
    @Getter
    @Setter
    private int device;

    /**
     * Timestamp
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
        dataOutput.writeInt(device);
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
        device = dataInput.readInt();
        timestamp = dataInput.readLong();
        interval = dataInput.readUTF();
    }

    /**
     * Implementation of compareTo() method.
     * Compares keys in the following order: 1->id, 2->timestamp
     * @param keyMapper the second object
     * @return -1, 0, 1 - less, equal or greater
     */
    @Override
    public int compareTo(KeyMapper keyMapper) {
        if (device == keyMapper.device)
            return Long.compare(timestamp, keyMapper.timestamp);
        return Integer.compare(device, keyMapper.device);
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

        KeyMapper keyMapper = (KeyMapper) o;

        if (device != keyMapper.device)
            return false;
        if (timestamp != keyMapper.timestamp)
            return false;
        return interval.equals(keyMapper.interval);
    }

    /**
     * Implementation of Object.hashCode() method
     * @return hash code
     */
    @Override
    public int hashCode() {
        int result = device;
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
        return "KeyMapper{ id=" + device +
                ", timestamp=" + timestamp +
                ", interval='" + interval + "'}";
    }
}
