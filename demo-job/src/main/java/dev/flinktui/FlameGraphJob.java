package dev.flinktui;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A continuously running workload with deliberate, recognizable call trees for flame-graph tests.
 *
 * <p>The central operator executes three differently shaped paths for every record: nested CPU
 * work, allocation-heavy audit rendering, and contended lock/wait work shared by all four
 * subtasks. This gives Flink's Mixed, On-CPU, and Off-CPU samplers useful data without depending
 * on production traffic or an accidental JVM hotspot.
 */
public final class FlameGraphJob {
    public static final String JOB_NAME = "Flink TUI Flame Lab";
    public static final String WORKLOAD_NAME = "03 Complex Flame Workload";

    private static final int PARALLELISM = 4;
    private static final String PAYLOAD = "profile-payload-" + "abcdef0123456789".repeat(32);

    private FlameGraphJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.disableOperatorChaining();
        env.enableCheckpointing(15_000);

        DataStream<String> events = env
                .addSource(new ProfileEventSource())
                .name("01 Profile Event Source")
                .uid("flame-profile-source")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> decoded = events
                .map(new DecodeProfileEvent())
                .name("02 Decode Profile Envelope")
                .uid("flame-decode-envelope")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> profiled = decoded
                .map(new ComplexFlameWorkload())
                .name(WORKLOAD_NAME)
                .uid("flame-complex-workload")
                .setParallelism(PARALLELISM);

        SingleOutputStreamOperator<String> signatures = profiled
                .keyBy(FlameGraphJob::tenantKey)
                .map(new AggregateSignature())
                .name("04 Aggregate Profile Signature")
                .uid("flame-aggregate-signature")
                .setParallelism(PARALLELISM);

        signatures
                .sinkTo(new DiscardingSink<>())
                .name("05 Flame Lab Sink")
                .uid("flame-lab-sink")
                .setParallelism(PARALLELISM);

        env.execute(JOB_NAME);
    }

    private static String tenantKey(String value) {
        int separator = value.indexOf('|');
        return separator < 0 ? value : value.substring(0, separator);
    }

    private static final class ProfileEventSource extends FixtureSource<String> {
        private volatile boolean running = true;
        private long sequence;

        @Override
        public void run(SourceContext<String> context) throws Exception {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            while (running) {
                long current = sequence++;
                int tenant = Math.floorMod((int) current + subtask * 7, 16);
                int amount = ThreadLocalRandom.current().nextInt(100, 10_000);
                synchronized (context.getCheckpointLock()) {
                    context.collect("tenant-" + tenant + "|" + subtask + "|" + current + "|" + amount + "|" + PAYLOAD);
                }
                Thread.sleep(6);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class DecodeProfileEvent implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            String[] fields = value.split("\\|", 5);
            return fields[0] + "|" + fields[1] + "|" + Long.parseLong(fields[2]) + "|"
                    + Integer.parseInt(fields[3]) + "|" + fields[4];
        }
    }

    /** The operator selected by integration tests and by humans exploring the Flame Graph view. */
    private static final class ComplexFlameWorkload implements MapFunction<String, String> {
        private static final ReentrantLock SHARED_MODEL_LOCK = new ReentrantLock(true);
        private static volatile long blackhole;

        @Override
        public String map(String value) {
            String[] fields = value.split("\\|", 5);
            long sequence = Long.parseLong(fields[2]);
            int amount = Integer.parseInt(fields[3]);

            long risk = runCpuPipeline(sequence, amount);
            String audit = runAllocationPipeline(fields[4], risk);
            long modelVersion = runContentionPipeline(sequence, risk);

            blackhole = risk ^ modelVersion ^ audit.hashCode();
            return fields[0] + "|" + sequence + "|" + risk + "|" + modelVersion + "|" + audit.length();
        }

        private long runCpuPipeline(long sequence, int amount) {
            long priced = evaluatePricingModel(sequence, amount);
            long fraud = evaluateFraudModel(priced, amount);
            return combineModelScores(priced, fraud);
        }

        private long evaluatePricingModel(long sequence, int amount) {
            long score = sequence ^ ((long) amount << 32);
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2);
            do {
                score = simulateMarketScenarios(score, amount);
            } while (System.nanoTime() < deadline);
            return score;
        }

        private long simulateMarketScenarios(long seed, int amount) {
            long value = seed;
            for (int scenario = 0; scenario < 2_048; scenario++) {
                value = mixRiskWindow(value + scenario * 0x9E3779B97F4A7C15L, amount);
            }
            return value;
        }

        private long mixRiskWindow(long value, int amount) {
            value ^= value >>> 30;
            value *= 0xBF58476D1CE4E5B9L;
            value ^= value >>> 27;
            value *= 0x94D049BB133111EBL;
            value ^= value >>> 31;
            return Long.rotateLeft(value, amount & 63);
        }

        private long evaluateFraudModel(long priced, int amount) {
            long score = priced;
            for (int rule = 0; rule < 8_192; rule++) {
                score ^= Long.rotateLeft(score + amount + rule, rule & 63);
                score = score * 31 + rule;
            }
            return score;
        }

        private long combineModelScores(long priced, long fraud) {
            return Long.rotateLeft(priced, 17) ^ Long.rotateRight(fraud, 11);
        }

        private String runAllocationPipeline(String payload, long risk) {
            List<String> tokens = tokenizeAuditDocument(payload, risk);
            List<String> fragments = buildAuditFragments(tokens, risk);
            return renderAuditDocument(fragments);
        }

        private List<String> tokenizeAuditDocument(String payload, long risk) {
            List<String> tokens = new ArrayList<>(128);
            for (int index = 0; index < 128; index++) {
                int start = Math.floorMod(index * 13, payload.length() - 24);
                tokens.add(payload.substring(start, start + 24) + '-' + Long.toHexString(risk + index));
            }
            return tokens;
        }

        private List<String> buildAuditFragments(List<String> tokens, long risk) {
            List<String> fragments = new ArrayList<>(tokens.size());
            for (int index = 0; index < tokens.size(); index++) {
                String token = tokens.get(index);
                fragments.add(index + ":" + token + ":" + Long.toUnsignedString(risk ^ token.hashCode(), 36));
            }
            return fragments;
        }

        private String renderAuditDocument(List<String> fragments) {
            StringBuilder document = new StringBuilder(fragments.size() * 56);
            for (String fragment : fragments) {
                document.append('{').append(fragment).append('}');
            }
            return document.toString();
        }

        private long runContentionPipeline(long sequence, long risk) {
            return waitForSharedModel(sequence, risk);
        }

        private long waitForSharedModel(long sequence, long risk) {
            SHARED_MODEL_LOCK.lock();
            try {
                return updateSharedModel(sequence, risk);
            } finally {
                SHARED_MODEL_LOCK.unlock();
            }
        }

        private long updateSharedModel(long sequence, long risk) {
            // Parking while holding a fair process-wide lock creates both a recognizable
            // TIMED_WAITING holder and contended WAITING callers for the Off-CPU sampler.
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            return Long.rotateLeft(risk ^ sequence ^ blackhole, 7);
        }
    }

    private static final class AggregateSignature implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            return value + "|signature=" + Integer.toUnsignedString(value.hashCode(), 36);
        }
    }
}
