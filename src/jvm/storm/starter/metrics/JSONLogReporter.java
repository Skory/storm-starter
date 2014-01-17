package storm.starter.metrics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class JSONLogReporter extends AbstractPollingReporter implements MetricProcessor<Map<String, Object>> {

    private static final String LOGGER_PREFIX = "metrics-json.";
    private MetricPredicate predicate;
    private Gson gson;

    /**
     * Enables the console reporter for the default metrics registry, and causes it to print to
     * STDOUT with the specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     */
    public static void enable(String serviceName, long period, TimeUnit unit) {
        enable(serviceName, Metrics.defaultRegistry(), period, unit);
    }

    /**
     * Enables the console reporter for the given metrics registry, and causes it to print to STDOUT
     * with the specified period and unrestricted output.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     */
    public static void enable(String serviceName, MetricsRegistry metricsRegistry, long period, TimeUnit unit) {
        enable(serviceName, metricsRegistry, period, unit, MetricPredicate.ALL);
    }

    /**
     * Enables the console reporter for the given metrics registry, and causes it to print to STDOUT
     * with the specified period and unrestricted output.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param predicate       the {@link MetricPredicate} used to determine whether a metric will be
     *                        output
     */
    public static void enable(String serviceName, MetricsRegistry metricsRegistry, long period, TimeUnit unit, MetricPredicate predicate) {
        final JSONLogReporter reporter = new JSONLogReporter(serviceName, metricsRegistry, predicate);
        reporter.start(period, unit);
    }


    private JSONLogReporter(String serviceName, MetricsRegistry registry, MetricPredicate predicate) {
        super(registry, "log-reporter");
        this.predicate = predicate;
        log = LoggerFactory.getLogger(LOGGER_PREFIX + serviceName);
        gson = new GsonBuilder()/*.setPrettyPrinting()*/.create();
    }

    @Override
    public void run() {
        try {
            Map<String, Object> metricData = new HashMap<>();
            for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
                for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                    metricData.clear();
                    put(metricData, "metric", entry.getKey());
                    put(metricData, "event", subEntry.getKey().getName());
                    subEntry.getValue().processWith(this, subEntry.getKey(), metricData);
                    log.info(gson.toJson(metricData));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Map<String, Object> data) throws Exception {
        put(data, "count", meter.count());
        put(data, "meanRate", meter.meanRate());
        put(data, "1minRate", meter.oneMinuteRate());
        put(data, "5minRate", meter.fiveMinuteRate());
        put(data, "15minRate", meter.fifteenMinuteRate());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Map<String, Object> data) throws Exception {
        put(data, "count", counter.count());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Map<String, Object> data) throws Exception {
        final Snapshot snapshot = histogram.getSnapshot();
        put(data, "min", histogram.min());
        put(data, "max", histogram.max());
        put(data, "mean", histogram.mean());
        put(data, "stddev", histogram.stdDev());
        put(data, "median", snapshot.getMedian());
        put(data, "75th", snapshot.get75thPercentile());
        put(data, "95th", snapshot.get95thPercentile());
        put(data, "99th", snapshot.get99thPercentile());
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Map<String, Object> data) throws Exception {
        processMeter(name, timer, data);
        final Snapshot snapshot = timer.getSnapshot();
        put(data, "min", timer.min());
        put(data, "max", timer.max());
        put(data, "mean", timer.mean());
        put(data, "median", snapshot.getMedian());
        put(data, "75th", snapshot.get75thPercentile());
        put(data, "95th", snapshot.get95thPercentile());
        put(data, "99th", snapshot.get99thPercentile());
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Map<String, Object> data) throws Exception {
        put(data, "value", gauge.value());
    }

    private void put(Map<String, Object> data, String name, int value){
        data.put(name, value);
    }

    private void put(Map<String, Object> data, String name, long value){
        data.put(name, value);
    }

    private void put(Map<String, Object> data, String name, double value){
        data.put(name, round(value));
    }

    private void put(Map<String, Object> data, String name, Object value){
        data.put(name, value);
    }

    private BigDecimal round(double val){
        return BigDecimal.valueOf(val).setScale(3, RoundingMode.HALF_UP);
    }

    private final Logger log;
}
