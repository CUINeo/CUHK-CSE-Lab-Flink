package cslab.flinklab;

import java.util.List;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class TwitterExample {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterExample.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        // Define the Twitter Source
        DataStream<String> streamSource = env.addSource(new CustomTwitterSource());

        // Transform
        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new GetWordCount())
                // group by words and sum their occurrences
                .keyBy(0)
                .sum(1);

        // emit result to the standard output
        SinkFunction<Tuple2<String, Integer>> printSink = new PrintSinkFunction<>();
        tweets.addSink(printSink).name("StdoutSink");

        // execute program
        env.execute("Twitter Streaming Example");
    }

    // *************************************************************************
    // Custom FlatMap Function
    // *************************************************************************
    public static class GetWordCount implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            // deserialize the tweet json to JsonNode object
            JsonNode jsonNode = jsonParser.readTree(value);

            // get the word count
            try {
                List<String> words = Arrays.asList(jsonNode.get("text").textValue().split(" "));
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            } catch (Exception e) {
                // LOG.info(e.toString());
            }
        }
    }
}
