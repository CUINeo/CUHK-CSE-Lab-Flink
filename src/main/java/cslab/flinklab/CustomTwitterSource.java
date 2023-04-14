package cslab.flinklab;

import java.util.Set;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twitter.clientlib.model.*;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.TwitterCredentialsBearer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomTwitterSource implements SourceFunction<String> {

    private boolean running;
    private int tweetId;
    private Set<String> tweetFields;
    private static final Logger LOG = LoggerFactory.getLogger(CustomTwitterSource.class);

    public CustomTwitterSource() {
        running = true;
        tweetId = 20;
        tweetFields = new HashSet<>();
        tweetFields.add("entities");
        tweetFields.add("text");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            try {
                TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(
                    "AAAAAAAAAAAAAAAAAAAAACjamgEAAAAA7YvTnzdlQBXLSOJ9C6cSKUWRCyk%3D4ntzSBJjWbz6bOUeWM6vsRdA3BbbhIQnkrVmtUDEPxfbnkk0Cv"
                    ));

                // Find tweets by Id
                Get2TweetsIdResponse result = apiInstance.tweets().findTweetById(String.valueOf(tweetId)).tweetFields(tweetFields).execute();
                tweetId ++;
                if (result.getErrors() == null && result.getData().toJson() != null) {
                    ctx.collect(result.getData().toJson());
                    Thread.sleep(1000);
                } else {
                    // LOG.info("Bad request");
                }

                // Search recent tweets
                // Get2TweetsSearchRecentResponse result = apiInstance.tweets().tweetsRecentSearch("NBA").tweetFields(tweetFields).maxResults(100).execute();
                // if (result.getData() != null) {
                //     for (int i = 0; i < result.getData().size(); i++) {
                //         ctx.collect(result.toJson());
                //         Thread.sleep(1000);
                //     }
                // } else {
                //     LOG.info("Bad request");
                // }                
            } catch (Exception e) {
                // LOG.info(e.toString());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}