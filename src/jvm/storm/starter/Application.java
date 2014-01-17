package storm.starter;

import com.yammer.metrics.core.MetricsRegistry;
import storm.starter.metrics.JSONLogReporter;
import storm.starter.metrics.LogReporter;

import java.util.concurrent.TimeUnit;

public class Application {
    private static final MetricsRegistry metrics = new MetricsRegistry();

    static {
        LogReporter.enable("topology", metrics, 10, TimeUnit.SECONDS);
        JSONLogReporter.enable("topology", metrics, 10, TimeUnit.SECONDS);
    }

    public static MetricsRegistry getMetrics() {
        return metrics;
    }
}
