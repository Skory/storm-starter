package twitter.streaming;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TwitterStreaming {
    private final BlockingQueue<String> msqQueue = new LinkedBlockingDeque<>(100000);
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingDeque<>(1000);
    private final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    private final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    private Authentication authentication;
    private Client client;
    private final Gson gson;
    private final Set<String> trendingSet = new HashSet<>();
    private final List<String> urlsToWrite = new ArrayList<>();
    private final List<String> tagsToWrite = new ArrayList<>();
    private int itemNumber = 0;
    private int flushItemsNumber = 0;
    private boolean saved = false;
    private final PrintWriter tagsWriter;
    private final PrintWriter urlsWriter;

    public TwitterStreaming(String tagsFilename, String urlsFilename, int flushItemsNumber) throws IOException {
        this.flushItemsNumber = flushItemsNumber;

        tagsWriter = new PrintWriter(new BufferedWriter(new FileWriter(tagsFilename, true)));
        urlsWriter = new PrintWriter(new BufferedWriter(new FileWriter(urlsFilename, true)));

        endpoint.addQueryParameter("locations", "-122.75,36.8,-121.75,37.8,-74,40,-73,41");
        authentication = new OAuth1("VPlUUepn4v6crs0zxE9Pg", "C3idVbC7Bxenzy7XNQexcGUGe5u9hoOVJelio7StJk",
                "2292448086-0ks5OavDhHiFKEB6xhhUl1eiBou21mmn32hYhpY", "SaG5iGL4IDgFdr9TrxHMG7wjWXpbbmQ6VtO0BBUa9U5DF");
        client = createClient();
        gson = new Gson();
    }

    private Client createClient() {
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("TwitterClient-1")
                .hosts(hosebirdHosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msqQueue))
                .retries(10)
                .socketTimeout(100000)
                .connectionTimeout(100000)
                .eventMessageQueue(eventQueue);

        return clientBuilder.build();
    }

    public void connect() {
        client.connect();
    }

    public void shutdown() {
        client.stop();
        tagsWriter.close();
        urlsWriter.close();
    }

    public void processQueue() throws InterruptedException {
        while (!client.isDone()) {
            String msg = msqQueue.take();
            Map map = gson.fromJson(msg, Map.class);
            Map entities = (Map) map.get("entities");
            if (entities != null) {
                List hashtags = (List) entities.get("hashtags");
                List urls = (List) entities.get("urls");

                if (!isEmpty(hashtags) && !isEmpty(urls)) {
                    continue;
                }

                if (!isEmpty(hashtags) || !isEmpty(urls)) {
                    saved = false;
                    itemNumber++;
                }

                if (!isEmpty(urls)) {
                    for (Object urlMap : urls) {
                        String url = ((Map) urlMap).get("expanded_url").toString();
                        if (trendingSet.add(url)) {
                            urlsToWrite.add(url);
                        }
                    }
                }

                if (!isEmpty(hashtags)) {
                    for (Object hashtag : hashtags) {
                        String tag = ((Map) hashtag).get("text").toString();
                        if (trendingSet.add(tag)) {
                            tagsToWrite.add(tag);
                        }
                    }
                }

                if (!saved && itemNumber % flushItemsNumber == 0) {
                    saved = true;
                    for (String s : tagsToWrite) {
                        tagsWriter.println(s);
                    }
                    tagsWriter.flush();
                    tagsToWrite.clear();

                    for (String s : urlsToWrite) {
                        urlsWriter.println(s);
                    }
                    urlsWriter.flush();
                    urlsToWrite.clear();
                }
            }

            System.out.println(itemNumber);
        }
    }

    private boolean isEmpty(List list) {
        return list != null && list.isEmpty();
    }
}
