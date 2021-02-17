import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
    public SortComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable key1 = (IntWritable) a;
        IntWritable key2 = (IntWritable) b;

        return -1 * key1.compareTo(key2);
    }
}
