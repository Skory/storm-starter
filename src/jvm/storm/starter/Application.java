package storm.starter;

import com.yammer.metrics.core.MetricsRegistry;
import storm.starter.metrics.LogReporter;

import java.util.concurrent.TimeUnit;

public class Application {
    private static final MetricsRegistry metrics = new MetricsRegistry();

    public static MetricsRegistry getMetrics() {
        return metrics;
    }

    public static void main(String[] args) throws Exception {
        LogReporter.enable("topology", metrics, 10, TimeUnit.SECONDS);
        TrendingTopology.main(args);
    }
}
