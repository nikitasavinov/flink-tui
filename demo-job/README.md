# Flink fixtures

The five jobs share their implementation across Flink 1.20 and 2.3. Build each
JAR against the matching runtime version; this is source compatibility, not a
single binary intended to run on both major versions.

With Java 17 and Maven:

```sh
mvn -Pflink-1 -Dflink.version=1.20.5 clean package
mvn -Pflink-2 -Dflink.version=2.3.0 clean package
```

Use `clean` when switching profiles so compiled classes from the other Flink
version cannot remain in the JAR. Docker builds select the profile from
`FLINK_VERSION` and start from an empty build directory:

```sh
docker build --build-arg FLINK_VERSION=1.20.5 .
```

The runtime image defaults to `flink:<version>-java17`. `FLINK_IMAGE` can
override it for a matching custom image.

The only version-specific source is `FixtureSource`: Flink 2 moved the legacy
`RichParallelSourceFunction` class into a `source.legacy` package. Maven includes
one adapter from `src/flink-1/java` or `src/flink-2/java`. Job topology, names,
state, checkpoint behavior, and the deliberate exception remain shared.
