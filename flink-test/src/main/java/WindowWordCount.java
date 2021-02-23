import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.core.fs.Path;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("./output/"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(60))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
                                .withMaxPartSize(1024 * 10)
                                .build())
                .build();

        DataStream<String> dataStream = env
//                .readTextFile("./input/input_0.csv")
                .socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .map(new Splitter())
                .keyBy(value -> value.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .reduce((ReduceFunction<Tuple2<String, OrderDetail>>) (t0, t1) -> {
                    t0.f1.quantity += t1.f1.quantity;
                    return new Tuple2<>(t0.f0, t0.f1);
                })
                .map((MapFunction<Tuple2<String, OrderDetail>, String>) tuple -> tuple.f1.toString());

        dataStream.addSink(sink);
//        dataStream.writeAsText("./output/" + getFileName() + ".txt");

        env.execute("Sum quantity");
    }

    public static String getFileName() {
        Timestamp time = new Timestamp(System.currentTimeMillis());
        return "output_" + time.toString();
    }

    public static class Splitter implements MapFunction<String, Tuple2<String, OrderDetail>> {
        @Override
        public Tuple2<String, OrderDetail> map(String s) {
            String[] values = s.split(",");
            OrderDetail detail = new OrderDetail();
            detail.orderId = Integer.parseInt(values[0]);
            detail.productId = Integer.parseInt(values[1]);
            detail.unitPrice = Float.parseFloat(values[2]);
            detail.quantity = Integer.parseInt(values[3]);
            detail.discount = Integer.parseInt(values[4]);
            return new Tuple2<>(values[0], detail);
        }
    }

}