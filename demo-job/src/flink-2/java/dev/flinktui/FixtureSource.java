package dev.flinktui;

import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;

/** Keeps fixture logic independent of the legacy source package move in Flink 2. */
abstract class FixtureSource<T> extends RichParallelSourceFunction<T> {}
