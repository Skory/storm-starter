package twitter.streaming;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            TwitterStreaming twitterStreaming = new TwitterStreaming("src/jvm/storm/starter/spout/tags.txt",
                    "src/jvm/storm/starter/spout/sites.txt", 10);

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
