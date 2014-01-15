package twitter.streaming;

import java.io.IOException;

public class Main {
    public static void main(String[] args){
        TwitterStreaming twitterStreaming = null;
        try {
            twitterStreaming = new TwitterStreaming("src/jvm/storm/starter/spout/sites.txt",
                    "src/jvm/storm/starter/spout/tags.txt", 10);

            twitterStreaming.connect();

            try {
                twitterStreaming.processQueue();
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterStreaming.shutdown();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
