# ZIO Stream Data Flow Pattern

The data flow pattern used in the integration tests is a prime example of a **ZIO Stream** pipeline, which is a powerful and declarative way to process data. It's designed to handle a sequence of operations in a non-blocking, resource-safe manner.

The pattern can be best understood as a series of connected stages, where the output of one stage becomes the input of the next.

## 1. The Source Stream

The process begins by creating a stream of file paths. This is the origin of our data flow.

```scala
ZStream.fromIterable(List(filePath1.toString, filePath2.toString))
```

This line creates a stream that emits the list of paths as our initial input.

## 2. The Per-File Processing Stage

This is the most critical part of the pipeline. The `.flatMap` operator is used to process each file path individually. For each path, it creates an entirely new sub-stream of the file's contents.

```scala
.flatMap(pathString =>
  ZStream.fromFileName(pathString)
    .via(ZPipeline.utf8Decode)
    .via(ZPipeline.splitLines)
    .drop(1)
)
```

Inside the `flatMap`, the following steps happen for each file:

- `ZStream.fromFileName(pathString)`: Reads the raw bytes of the file.
- `.via(ZPipeline.utf8Decode)`: Decodes the bytes into a single string.
- `.via(ZPipeline.splitLines)`: Splits that string into a stream of individual lines.
- `.drop(1)`: **Crucially**, it drops the very first line of this new stream, which is the header. This prevents the header from being sent to the parser.

The `.flatMap` operation effectively "flattens" these individual streams of lines into one continuous stream, where all the header lines have been removed.

## 3. The Parsing Stage

After the headers are dropped, the continuous stream of data lines is sent to the parser.

```scala
.via(BufrParserPipeline())
```

The `BufrParserPipeline` takes the stream of raw `String` lines and transforms each one into a `BufrCodeFlagEntry` case class. This is where the raw data is given structure and meaning. If a line is malformed, this is where the pipeline would fail.

## 4. The Aggregation Sink

The final stage is the "sink," which is responsible for consuming all the processed data and producing a final result.

```scala
.run(BufrCodeFlagAggregator.aggregateToMapTyped())
```

The `aggregateToMapTyped()` function acts as the sink. It receives the stream of `BufrCodeFlagEntry` objects and builds the final `Map`. It handles the core logic of the integration test:

- It uses the `BufrCodeFlagKey` (which consists of `fxy` and `codeFigure`) to uniquely identify each entry.
- Because `ZStream` processes the files sequentially, the sink will receive the entry from `file2Content` for `001007,1` *after* the one from `file1Content`. The aggregator's logic ensures that this later entry **overwrites** the earlier one, which is why the final map contains the "ERS 1 (DUPLICATE)" entry.

The entire process is a seamless, resource-safe pipeline that takes multiple source files, cleans their data, parses them, and aggregates them into a final, structured data model.