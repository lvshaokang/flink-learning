import com.lsk.bigdata.fink.kafka.HdfsSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * TODO:
 *
 * @author red
 * @class_name TestApp
 * @date 2020-09-16
 */
public class TestApp {

    public static void main(String[] args) {
        StreamingFileSink<String> sink = StreamingFileSink
                //.forRowFormat(new Path("file:///E://binlog_db/city"), new SimpleStringEncoder<String>())
                .forRowFormat(new Path("hdfs://kms-1:8020/binlog_db/code_city_delta"), new SimpleStringEncoder<String>())
                .withBucketAssigner(new HdfsSink.EventTimeBucketAssigner())
                .withRollingPolicy(null)
                .withBucketCheckInterval(1000)  // 桶检查间隔，这里设置1S
                .build();
    }
}
