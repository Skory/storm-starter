package storm.starter.metrics;

import com.yammer.metrics.core.Metric;
import storm.starter.util.Action1;

import java.util.HashMap;
import java.util.Map;

public class MetricsManager {
    private static Map<String, Metric> stringMeteredMap = new HashMap<>();

    public static void register(String name, Metric metered) {
        stringMeteredMap.put(name, metered);
    }

    public static <T extends Metric>
    void interactWith(String metricName, Action1<T> action) {
        Metric metered = stringMeteredMap.get(metricName);
        if (metered != null) {
            action.invoke((T)metered);
        }
    }
}
