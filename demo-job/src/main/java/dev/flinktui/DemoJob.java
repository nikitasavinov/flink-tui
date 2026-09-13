package dev.flinktui;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/** A deliberately shaped, continuously running graph for the TUI prototype. */
public final class DemoJob {
    private static final String PAYLOAD = "x".repeat(4096);

    private DemoJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.enableCheckpointing(10_000);

        DataStream<String> orders = env
                .addSource(new OrdersSource())
                .name("Orders Source")
                .uid("orders-source")
                .setParallelism(2);

        // 2-4: shared ingestion path.
        SingleOutputStreamOperator<String> decoded = stage(orders, "Decode Orders", "decode-orders", "DECODED");
        SingleOutputStreamOperator<String> validated = stage(decoded, "Validate Orders", "validate-orders", "VALID");
        SingleOutputStreamOperator<String> enriched = stage(validated, "Enrich Customers", "enrich-customers", "ENRICHED");

        // 5-9: revenue branch.
        SingleOutputStreamOperator<String> extractedRevenue = stage(
                enriched.keyBy(DemoJob::customerKey), "Extract Revenue", "extract-revenue", "REVENUE");
        SingleOutputStreamOperator<String> rollingRevenue = stage(
                extractedRevenue, "Rolling Revenue", "rolling-revenue", "ROLLING");
        SingleOutputStreamOperator<String> taxedRevenue = stage(
                rollingRevenue, "Apply Tax", "apply-tax", "TAXED");
        SingleOutputStreamOperator<String> convertedRevenue = stage(
                taxedRevenue, "Convert Currency", "convert-currency", "CONVERTED");
        SingleOutputStreamOperator<String> revenueSummary = stage(
                convertedRevenue, "Revenue Summary", "revenue-summary", "REVENUE-SUMMARY");

        // 10-14: risk branch. Risk Score is the deliberate bottleneck.
        SingleOutputStreamOperator<String> riskFeatures = stage(
                enriched.rebalance(), "Risk Features", "risk-features", "FEATURES");
        SingleOutputStreamOperator<String> riskScore = riskFeatures
                .rebalance()
                .map(new SlowRiskScore())
                .name("Risk Score")
                .uid("risk-score")
                .setParallelism(2);
        SingleOutputStreamOperator<String> fraudRules = stage(
                riskScore, "Fraud Rules", "fraud-rules", "RULED");
        SingleOutputStreamOperator<String> normalizedRisk = stage(
                fraudRules, "Normalize Risk", "normalize-risk", "NORMALIZED-RISK");
        SingleOutputStreamOperator<String> riskSummary = stage(
                normalizedRisk, "Risk Summary", "risk-summary", "RISK-SUMMARY");

        // 15-19: inventory branch.
        SingleOutputStreamOperator<String> inventoryKey = stage(
                enriched.keyBy(DemoJob::customerKey), "Inventory Key", "inventory-key", "INVENTORY-KEY");
        SingleOutputStreamOperator<String> inventoryLookup = stage(
                inventoryKey, "Inventory Lookup", "inventory-lookup", "LOOKED-UP");
        SingleOutputStreamOperator<String> stockCheck = stage(
                inventoryLookup, "Stock Check", "stock-check", "IN-STOCK");
        SingleOutputStreamOperator<String> fulfillmentPlan = stage(
                stockCheck, "Fulfillment Plan", "fulfillment-plan", "PLANNED");
        SingleOutputStreamOperator<String> inventorySummary = stage(
                fulfillmentPlan, "Inventory Summary", "inventory-summary", "INVENTORY-SUMMARY");

        // 20-23: merge the three branches back into a decision stream.
        SingleOutputStreamOperator<String> revenueAndRisk = join(
                revenueSummary, riskSummary, "Join Revenue Risk", "join-revenue-risk", "FINANCIAL");
        SingleOutputStreamOperator<String> financialDecision = stage(
                revenueAndRisk, "Financial Decision", "financial-decision", "DECIDED");
        SingleOutputStreamOperator<String> withInventory = join(
                financialDecision, inventorySummary, "Join Inventory", "join-inventory", "COMBINED");
        SingleOutputStreamOperator<String> routed = stage(
                withInventory, "Route Decision", "route-decision", "ROUTED");

        // 24-30: three output paths.
        SingleOutputStreamOperator<String> auditFormat = stage(
                routed, "Audit Format", "audit-format", "AUDIT");
        auditFormat.sinkTo(new DiscardingSink<>())
                .name("Audit Sink")
                .uid("audit-sink")
                .setParallelism(1);

        SingleOutputStreamOperator<String> orderFormat = stage(
                routed, "Order Format", "order-format", "ORDER");
        orderFormat.sinkTo(new DiscardingSink<>())
                .name("Orders Sink")
                .uid("orders-sink")
                .setParallelism(1);

        SingleOutputStreamOperator<String> alerts = routed
                .filter(value -> Math.floorMod(value.hashCode(), 4) == 0)
                .name("Alert Filter")
                .uid("alert-filter")
                .setParallelism(2);
        SingleOutputStreamOperator<String> alertFormat = stage(
                alerts, "Alert Format", "alert-format", "ALERT");
        alertFormat.sinkTo(new DiscardingSink<>())
                .name("Alerts Sink")
                .uid("alerts-sink")
                .setParallelism(1);

        env.execute("Flink TUI Demo");
    }

    private static SingleOutputStreamOperator<String> stage(
            DataStream<String> input, String name, String uid, String label) {
        return input
                .map(new LabelStage(label))
                .name(name)
                .uid(uid)
                .setParallelism(2);
    }

    private static SingleOutputStreamOperator<String> join(
            DataStream<String> left,
            DataStream<String> right,
            String name,
            String uid,
            String label) {
        return left
                .connect(right)
                .flatMap(new CorrelateSignals(label))
                .name(name)
                .uid(uid)
                .setParallelism(2);
    }

    private static String customerKey(String value) {
        int separator = value.indexOf('|');
        return separator < 0 ? value : value.substring(0, separator);
    }

    private static final class OrdersSource extends FixtureSource<String> {
        private volatile boolean running = true;
        private long sequence;

        @Override
        public void run(SourceContext<String> context) throws Exception {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            while (running) {
                long id = sequence++;
                int customer = Math.floorMod((int) id + subtask * 17, 50);
                int amount = ThreadLocalRandom.current().nextInt(10, 500);
                long timestamp = System.currentTimeMillis();
                synchronized (context.getCheckpointLock()) {
                    context.collectWithTimestamp(
                            "customer-" + customer + "|order-" + subtask + "-" + id + "|" + amount + "|" + PAYLOAD,
                            timestamp);
                    if (id % 100 == 0) {
                        context.emitWatermark(new Watermark(timestamp - 1_000));
                    }
                }
                Thread.sleep(5);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class SlowRiskScore implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            // The rate and payload gap fill network buffers quickly, producing real
            // upstream backpressure for the TUI instead of only a busy bottleneck.
            Thread.sleep(80);
            return "RISK|" + value;
        }
    }

    private static final class LabelStage implements MapFunction<String, String> {
        private final String label;

        private LabelStage(String label) {
            this.label = label;
        }

        @Override
        public String map(String value) {
            return label + "|" + value;
        }
    }

    private static final class CorrelateSignals implements CoFlatMapFunction<String, String, String> {
        private final String label;

        private CorrelateSignals(String label) {
            this.label = label;
        }

        @Override
        public void flatMap1(String left, Collector<String> out) {
            out.collect(label + "|LEFT|" + left);
        }

        @Override
        public void flatMap2(String right, Collector<String> out) {
            out.collect(label + "|RIGHT|" + right);
        }
    }
}
