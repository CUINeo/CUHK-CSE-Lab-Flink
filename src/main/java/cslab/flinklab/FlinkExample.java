package cslab.flinklab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class FlinkExample {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkExample.class);

    public static void main(String[] args) throws Exception {
        // checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // define the Source
        DataStream<String> streamSource = env.addSource(new CustomSource());

        // transform
        DataStream<Tuple2<String, Integer>> words = streamSource
                // splitting sentences to (word, 1)
                .flatMap(new GetWordCount())
                // group by words and sum their occurrences
                .keyBy(0)
                .sum(1);

        // emit result to the standard output
        SinkFunction<Tuple2<String, Integer>> printSink = new PrintSinkFunction<>();
        words.addSink(printSink).name("StdoutSink");

        // execute program
        env.execute("Flink Streaming Example");
    }

    // *************************************************************************
    // Custom FlatMap Function
    // *************************************************************************
    public static class GetWordCount implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // split the sentence into words
            String[] words = value.toLowerCase().split("\\W+");

            // emit each word with a count of 1
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
