package dev.flinktui;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.runtime.state.CheckpointStorageFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

/**
 * A small, deterministic job used to exercise checkpoint diagnostics in the TUI.
 *
 * <p>Operator chaining is disabled deliberately: the execution graph must always contain exactly
 * five vertices. Stable names and UIDs make the job suitable for future integration assertions.
 */
public final class CheckpointJob {
    public static final String JOB_NAME = "Flink TUI Checkpoints";

    private static final int PARALLELISM = 2;
    private static final int ACCOUNT_COUNT = 128;

    private CheckpointJob() {}

    public static void main(String[] args) throws Exception {
        boolean failCheckpoints = args.length > 0 && "--fail-checkpoints".equals(args[0]);
        Configuration configuration = new Configuration();
        if (failCheckpoints) {
            configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, LimitedCheckpointStorage.class.getName());
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);
        env.disableOperatorChaining();
        env.enableCheckpointing(failCheckpoints ? 3_600_000 : 3_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(1_000);
        checkpoints.setCheckpointTimeout(20_000);
        checkpoints.setMaxConcurrentCheckpoints(1);
        if (failCheckpoints) {
            // Let E2E trigger each failure explicitly and keep the fixture running afterward.
            checkpoints.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        }

        DataStream<String> events = env
                .addSource(new CheckpointedTransactionSource())
                .name("01 Transaction Source")
                .uid("checkpoint-transaction-source")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> decoded = events
                .map(new DecodeTransaction())
                .name("02 Decode Transaction")
                .uid("checkpoint-decode-transaction")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> balances = decoded
                .keyBy(CheckpointJob::accountKey)
                .map(new RunningBalance())
                .name("03 Account Balance State")
                .uid("checkpoint-account-balance")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> classified = balances
                .map(new ClassifyBalance())
                .name("04 Classify Balance")
                .uid("checkpoint-classify-balance")
                .setParallelism(PARALLELISM);

        classified
                .sinkTo(new DiscardingSink<>())
                .name("05 Checkpoint Sink")
                .uid("checkpoint-sink")
                .setParallelism(PARALLELISM);

        env.execute(failCheckpoints ? "Flink TUI Checkpoint Failure Lab" : JOB_NAME);
    }

    /** Loaded by Flink from the E2E fixture's checkpoint storage configuration. */
    public static final class LimitedCheckpointStorage implements CheckpointStorageFactory<JobManagerCheckpointStorage> {
        @Override
        public JobManagerCheckpointStorage createFromConfig(ReadableConfig configuration, ClassLoader classLoader) {
            // Even the source's sequence state exceeds this one-byte storage limit.
            return new JobManagerCheckpointStorage(1);
        }
    }

    private static String accountKey(String event) {
        int separator = event.indexOf('|');
        return separator < 0 ? event : event.substring(0, separator);
    }

    /** A replayable source with operator state so the source itself contributes to checkpoints. */
    private static final class CheckpointedTransactionSource
            extends FixtureSource<String> implements CheckpointedFunction {
        private transient ListState<Long> sequenceState;
        private volatile boolean running = true;
        private long sequence;

        @Override
        public void run(SourceContext<String> context) throws Exception {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            while (running) {
                long timestamp = System.currentTimeMillis();
                synchronized (context.getCheckpointLock()) {
                    long current = sequence++;
                    int account = Math.floorMod((int) (current * 13 + subtask * 61), ACCOUNT_COUNT);
                    int delta = Math.floorMod((int) (current * 37 + subtask * 17), 201) - 100;
                    if (delta == 0) {
                        delta = 1;
                    }
                    context.collectWithTimestamp(
                            "account-" + account + "|" + delta + "|" + subtask + "-" + current + "|" + timestamp,
                            timestamp);
                    if (current % 20 == 0) {
                        context.emitWatermark(new Watermark(timestamp - 500));
                    }
                }
                Thread.sleep(25);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            sequenceState.clear();
            sequenceState.add(sequence);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            sequenceState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("transaction-sequence", Long.class));
            if (context.isRestored()) {
                for (Long restoredSequence : sequenceState.get()) {
                    sequence = Math.max(sequence, restoredSequence);
                }
            }
        }
    }

    /** Normalizes source records while keeping the wire format intentionally transparent. */
    private static final class DecodeTransaction implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            String[] fields = value.split("\\|", 4);
            return fields[0] + "|" + Long.parseLong(fields[1]) + "|" + fields[2] + "|" + fields[3];
        }
    }

    /** Keyed managed state makes every completed checkpoint contain meaningful state. */
    private static final class RunningBalance extends RichMapFunction<String, String> {
        private transient ValueState<Long> balance;
        private transient ValueState<Long> transactionCount;
        private transient ValueState<String> recentTransactionIDs;

        @Override
        public void open(OpenContext openContext) {
            balance = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Long.class));
            transactionCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("transaction-count", Long.class));
            recentTransactionIDs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("recent-transaction-ids", String.class));
        }

        @Override
        public String map(String value) throws Exception {
            String[] fields = value.split("\\|", 4);
            long nextBalance = valueOrZero(balance.value()) + Long.parseLong(fields[1]);
            long nextCount = valueOrZero(transactionCount.value()) + 1;
            balance.update(nextBalance);
            transactionCount.update(nextCount);

            String previousIDs = recentTransactionIDs.value();
            String nextIDs = previousIDs == null || previousIDs.isEmpty()
                    ? fields[2]
                    : previousIDs + "," + fields[2];
            if (nextIDs.length() > 256) {
                nextIDs = nextIDs.substring(nextIDs.length() - 256);
            }
            recentTransactionIDs.update(nextIDs);

            return fields[0] + "|" + nextBalance + "|" + nextCount + "|" + fields[3];
        }

        private static long valueOrZero(Long value) {
            return value == null ? 0 : value;
        }
    }

    private static final class ClassifyBalance implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            String[] fields = value.split("\\|", 4);
            long balance = Long.parseLong(fields[1]);
            String classification = balance < -500 ? "OVERDRAWN" : balance > 500 ? "FUNDED" : "NORMAL";
            return classification + "|" + value;
        }
    }
}
