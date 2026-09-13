package dev.flinktui;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * A topology fixture designed specifically to stress terminal graph layout and navigation.
 *
 * <p>The graph has five independent entries, eight exits, repeated fan-out/fan-in, and two
 * deliberately long cross-layer edges. Operator chaining is disabled and every operator has a
 * stable UID so its physical execution graph remains useful for visual regression testing.
 */
public final class TopologyLabJob {
    public static final String JOB_NAME = "Flink TUI Topology Lab";

    private static final int PARALLELISM = 1;

    private TopologyLabJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.disableOperatorChaining();
        env.enableCheckpointing(15_000);

        // Rank 0: five independent entry points with deliberately different rates.
        DataStream<String> webOrders = source(env, "01 Web Orders Source", "lab-web-source", "WEB", 35);
        DataStream<String> storeOrders = source(env, "02 Store Orders Source", "lab-store-source", "STORE", 55);
        DataStream<String> payments = source(env, "03 Payments Source", "lab-payments-source", "PAYMENT", 45);
        DataStream<String> inventory = source(env, "04 Inventory Source", "lab-inventory-source", "INVENTORY", 70);
        DataStream<String> rules = source(env, "05 Rules Source", "lab-rules-source", "RULE", 120);

        // Rank 1: every entry has its own ingestion path. Web orders also terminate early.
        SingleOutputStreamOperator<String> decodedWeb = stage(
                webOrders, "06 Decode Web Orders", "lab-decode-web", "WEB-DECODED");
        SingleOutputStreamOperator<String> decodedStore = stage(
                storeOrders, "07 Decode Store Orders", "lab-decode-store", "STORE-DECODED");
        SingleOutputStreamOperator<String> normalizedPayments = stage(
                payments, "08 Normalize Payments", "lab-normalize-payments", "PAYMENT-NORMALIZED");
        SingleOutputStreamOperator<String> normalizedInventory = stage(
                inventory, "09 Normalize Inventory", "lab-normalize-inventory", "INVENTORY-NORMALIZED");
        SingleOutputStreamOperator<String> parsedRules = stage(
                rules, "10 Parse Rules", "lab-parse-rules", "RULE-PARSED");
        sink(webOrders, "11 Raw Web Archive Sink", "lab-raw-web-sink");

        // Rank 2: the first true many-to-one input plus three independent preparation branches.
        SingleOutputStreamOperator<String> unifiedOrders = stage(
                decodedWeb.union(decodedStore),
                "12 Unified Orders",
                "lab-unified-orders",
                "ORDER-UNIFIED");
        SingleOutputStreamOperator<String> paymentAuthorization = stage(
                normalizedPayments,
                "13 Payment Authorization",
                "lab-payment-authorization",
                "PAYMENT-AUTHORIZED");
        SingleOutputStreamOperator<String> inventorySnapshot = stage(
                normalizedInventory,
                "14 Inventory Snapshot",
                "lab-inventory-snapshot",
                "INVENTORY-SNAPSHOT");
        SingleOutputStreamOperator<String> ruleIndex = stage(
                parsedRules, "15 Rule Index", "lab-rule-index", "RULE-INDEXED");

        // Rank 3: Unified Orders fans into three joins; two other branches end at shallower depths.
        SingleOutputStreamOperator<String> availability = merge(
                unifiedOrders,
                inventorySnapshot,
                "16 Enrich Availability",
                "lab-enrich-availability",
                "AVAILABILITY");
        SingleOutputStreamOperator<String> ordersWithPayment = merge(
                unifiedOrders,
                paymentAuthorization,
                "17 Attach Payment",
                "lab-attach-payment",
                "PAID-ORDER");
        SingleOutputStreamOperator<String> riskContext = merge(
                unifiedOrders,
                ruleIndex,
                "18 Score Risk",
                "lab-score-risk",
                "RISK-CONTEXT");
        SingleOutputStreamOperator<String> inventoryChanges = stage(
                inventorySnapshot,
                "19 Inventory Change Feed",
                "lab-inventory-change-feed",
                "INVENTORY-CHANGE");
        sink(paymentAuthorization, "20 Payment Audit Sink", "lab-payment-audit-sink");

        // Rank 4: two decision branches and one analytics branch converge different parents.
        SingleOutputStreamOperator<String> fulfillment = merge(
                availability,
                ordersWithPayment,
                "21 Fulfillment Plan",
                "lab-fulfillment-plan",
                "FULFILLMENT");
        SingleOutputStreamOperator<String> riskDecision = merge(
                ordersWithPayment,
                riskContext,
                "22 Risk Decision",
                "lab-risk-decision",
                "RISK-DECISION");
        SingleOutputStreamOperator<String> liveAnalytics = merge(
                availability,
                riskContext,
                "23 Live Analytics",
                "lab-live-analytics",
                "ANALYTICS");
        sink(inventoryChanges, "24 Inventory Update Sink", "lab-inventory-update-sink");

        // Rank 5: primary convergence. Payment Authorization also jumps across three layers.
        SingleOutputStreamOperator<String> finalDecision = merge(
                fulfillment,
                riskDecision,
                "25 Final Decision",
                "lab-final-decision",
                "FINAL");
        SingleOutputStreamOperator<String> analyticsAggregate = merge(
                liveAnalytics,
                paymentAuthorization,
                "26 Analytics Aggregate",
                "lab-analytics-aggregate",
                "ANALYTICS-AGGREGATE");

        // Rank 6: Final Decision fans out four ways. Rule Index creates another long edge.
        SingleOutputStreamOperator<String> approved = route(
                finalDecision, "27 Approved Route", "lab-approved-route", 0);
        SingleOutputStreamOperator<String> rejected = route(
                finalDecision, "28 Rejected Route", "lab-rejected-route", 1);
        SingleOutputStreamOperator<String> review = route(
                finalDecision, "29 Manual Review Route", "lab-review-route", 2);
        SingleOutputStreamOperator<String> decisionAudit = merge(
                finalDecision,
                ruleIndex,
                "30 Decision Audit",
                "lab-decision-audit",
                "DECISION-AUDIT");
        sink(analyticsAggregate, "31 Analytics Sink", "lab-analytics-sink");

        // Rank 7: four deep exits complement the four earlier exits above.
        sink(approved, "32 Approved Orders Sink", "lab-approved-sink");
        sink(rejected, "33 Rejected Orders Sink", "lab-rejected-sink");
        sink(review, "34 Manual Review Sink", "lab-review-sink");
        sink(decisionAudit, "35 Decision Audit Sink", "lab-decision-audit-sink");

        env.execute(JOB_NAME);
    }

    private static DataStream<String> source(
            StreamExecutionEnvironment env, String name, String uid, String label, long delayMillis) {
        return env.addSource(new SyntheticSource(label, delayMillis))
                .name(name)
                .uid(uid)
                .setParallelism(PARALLELISM);
    }

    private static SingleOutputStreamOperator<String> stage(
            DataStream<String> input, String name, String uid, String label) {
        return input.map(new LabelStage(label))
                .name(name)
                .uid(uid)
                .setParallelism(PARALLELISM);
    }

    private static SingleOutputStreamOperator<String> merge(
            DataStream<String> left,
            DataStream<String> right,
            String name,
            String uid,
            String label) {
        return left.connect(right)
                .flatMap(new MergeSignals(label))
                .name(name)
                .uid(uid)
                .setParallelism(PARALLELISM);
    }

    private static SingleOutputStreamOperator<String> route(
            DataStream<String> input, String name, String uid, int bucket) {
        return input.filter(new RouteFilter(bucket))
                .name(name)
                .uid(uid)
                .setParallelism(PARALLELISM);
    }

    private static void sink(DataStream<String> input, String name, String uid) {
        input.sinkTo(new DiscardingSink<>())
                .name(name)
                .uid(uid)
                .setParallelism(PARALLELISM);
    }

    private static final class SyntheticSource extends FixtureSource<String> {
        private final String label;
        private final long delayMillis;
        private volatile boolean running = true;
        private long sequence;

        private SyntheticSource(String label, long delayMillis) {
            this.label = label;
            this.delayMillis = delayMillis;
        }

        @Override
        public void run(SourceContext<String> context) throws Exception {
            while (running) {
                long current = sequence++;
                long timestamp = System.currentTimeMillis();
                synchronized (context.getCheckpointLock()) {
                    context.collectWithTimestamp(label + "|" + current + "|" + timestamp, timestamp);
                    if (current % 50 == 0) {
                        context.emitWatermark(new Watermark(timestamp - 500));
                    }
                }
                Thread.sleep(delayMillis);
            }
        }

        @Override
        public void cancel() {
            running = false;
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

    private static final class MergeSignals implements CoFlatMapFunction<String, String, String> {
        private final String label;

        private MergeSignals(String label) {
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

    private static final class RouteFilter implements FilterFunction<String> {
        private final int bucket;

        private RouteFilter(int bucket) {
            this.bucket = bucket;
        }

        @Override
        public boolean filter(String value) {
            return Math.floorMod(value.hashCode(), 3) == bucket;
        }
    }
}
