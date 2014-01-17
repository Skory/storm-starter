package storm.starter.metrics;

import com.google.common.base.Strings;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class LogReporter extends AbstractPollingReporter implements MetricProcessor<StringBuilder> {

    private static final String LOGGER_PREFIX = "metrics.";
    private MetricPredicate predicate;

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
        final LogReporter reporter = new LogReporter(serviceName, metricsRegistry, predicate);
        reporter.start(period, unit);
    }


    private LogReporter(String serviceName, MetricsRegistry registry, MetricPredicate predicate) {
        super(registry, "log-reporter");
        this.predicate = predicate;
        log = LoggerFactory.getLogger(LOGGER_PREFIX + serviceName);
    }

    @Override
    public void run() {
        try {
            StringBuilder builder = new StringBuilder();
            String delimiter = Strings.repeat("*", 25);

            builder.append('\n').append(delimiter).append('\n');
            builder.append("System stats on ").append(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm").print(DateTime.now()));
            builder.append('\n').append(delimiter).append('\n');

            for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(
                    predicate).entrySet()) {
                builder.append(entry.getKey());
                builder.append(':').append('\n');
                for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                    builder.append("  ");
                    builder.append(subEntry.getKey().getName());
                    builder.append(':');
                    subEntry.getValue().processWith(this, subEntry.getKey(), builder);
                }
            }
            log.info(builder.toString());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, StringBuilder builder) throws Exception {
        final String unit = abbrev(meter.rateUnit());
        print(builder, " %d\n", meter.count());
        print(builder, "    mean rate = %2.2f %s/%s\n",
                meter.meanRate(),
                meter.eventType(),
                unit);
        print(builder, "    1-minute rate = %2.2f %s/%s\n",
                meter.oneMinuteRate(),
                meter.eventType(),
                unit);
        print(builder, "    5-minute rate = %2.2f %s/%s\n",
                meter.fiveMinuteRate(),
                meter.eventType(),
                unit);
        print(builder, "    15-minute rate = %2.2f %s/%s\n",
                meter.fifteenMinuteRate(),
                meter.eventType(),
                unit);
    }

    @Override
    public void processCounter(MetricName name, Counter counter, StringBuilder builder) throws Exception {
        print(builder, " %d\n", counter.count());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, StringBuilder builder) throws Exception {
        final Snapshot snapshot = histogram.getSnapshot();
        builder.append("\n");
        print(builder, "    min = %2.2f\n", histogram.min());
        print(builder, "    max = %2.2f\n", histogram.max());
        print(builder, "    mean = %2.2f\n", histogram.mean());
        print(builder, "    stddev = %2.2f\n", histogram.stdDev());
        print(builder, "    median = %2.2f\n", snapshot.getMedian());
        print(builder, "    75%% <= %2.2f\n", snapshot.get75thPercentile());
        print(builder, "    95%% <= %2.2f\n", snapshot.get95thPercentile());
    }

    @Override
    public void processTimer(MetricName name, Timer timer, StringBuilder builder) throws Exception {
        processMeter(name, timer, builder);
        final String durationUnit = abbrev(timer.durationUnit());
        final Snapshot snapshot = timer.getSnapshot();
        print(builder, "    min = %2.2f%s\n", timer.min(), durationUnit);
        print(builder, "    max = %2.2f%s\n", timer.max(), durationUnit);
        print(builder, "    mean = %2.2f%s\n", timer.mean(), durationUnit);
        print(builder, "    stddev = %2.2f%s\n", timer.stdDev(), durationUnit);
        print(builder, "    median = %2.2f%s\n", snapshot.getMedian(), durationUnit);
        print(builder, "    75%% <= %2.2f%s\n", snapshot.get75thPercentile(), durationUnit);
        print(builder, "    95%% <= %2.2f%s\n", snapshot.get95thPercentile(), durationUnit);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, StringBuilder builder) throws Exception {
        print(builder, " %s\n", gauge.value());
    }

    private void print(StringBuilder sb, String format, Object... args) {
        sb.append(String.format(format, args));
    }

    private String abbrev(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "us";
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new RuntimeException("Unrecognized TimeUnit: " + unit);
        }
    }

    private final Logger log;
}
