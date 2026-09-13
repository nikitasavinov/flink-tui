package dev.flinktui;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

/**
 * E2E-only incident fixture: fail once, then keep the recovered execution running.
 *
 * <p>The retained failure and live worker let the TUI test graph, thread, log, and
 * TaskManager navigation without depending on an incidental playground failure.
 */
public final class ExceptionJob {
    public static final String JOB_NAME = "Flink TUI Exception Lab";

    private ExceptionJob() {}

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("restart-strategy.type", "fixed-delay");
        configuration.setString("restart-strategy.fixed-delay.attempts", "3");
        configuration.setString("restart-strategy.fixed-delay.delay", "1 s");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.disableOperatorChaining();

        env.addSource(new IncidentSource())
                .name("01 Incident Source")
                .uid("exception-source")
                .map(new FailOnce())
                .name("02 Fail Once")
                .uid("exception-fail-once")
                .sinkTo(new DiscardingSink<>())
                .name("03 Recovered Sink")
                .uid("exception-sink");

        env.execute(JOB_NAME);
    }

    private static final class IncidentSource extends FixtureSource<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> context) throws Exception {
            long sequence = 0;
            while (running) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(sequence++);
                }
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class FailOnce extends RichMapFunction<Long, Long> {
        @Override
        public Long map(Long value) {
            if (getRuntimeContext().getTaskInfo().getAttemptNumber() == 0 && value >= 5) {
                throw new IllegalStateException("Intentional Flink TUI E2E incident");
            }
            return value;
        }
    }
}
