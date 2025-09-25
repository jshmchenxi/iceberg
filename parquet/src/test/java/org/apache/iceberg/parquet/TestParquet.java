/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.write;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquet {

  @TempDir private Path temp;

  @Test
  public void testRowGroupSizeConfigurable() throws IOException {
    // Without an explicit writer function doesn't support PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT
    // PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT configs.
    // Even though row group size is 16 bytes, we still have to write 101 records
    // as default PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT is 100.
    File parquetFile = generateFile(null, 101, 4 * Integer.BYTES, null, null).first();

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      assertThat(reader.getRowGroups()).hasSize(2);
    }
  }

  @Test
  public void testRowGroupSizeConfigurableWithWriter() throws IOException {
    // Explicit writer function supports PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT
    // and PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT configs.
    // We should just need to write 5 integers (20 bytes)
    // to create two row groups with row group size configured at 16 bytes.
    File parquetFile =
        generateFile(ParquetAvroWriter::buildWriter, 5, 4 * Integer.BYTES, 1, 2).first();

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      assertThat(reader.getRowGroups()).hasSize(2);
    }
  }

  @Test
  public void testMetricsMissingColumnStatisticsInRowGroups() throws IOException {
    Schema schema = new Schema(optional(1, "stringCol", Types.StringType.get()));

    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(1);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    GenericData.Record smallRecord = new GenericData.Record(avroSchema);
    smallRecord.put("stringCol", "test");
    records.add(smallRecord);

    GenericData.Record largeRecord = new GenericData.Record(avroSchema);
    largeRecord.put("stringCol", Strings.repeat("a", 2048));
    records.add(largeRecord);

    write(
        file,
        schema,
        ImmutableMap.<String, String>builder()
            .put(PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .put(PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .put(PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
            .buildOrThrow(),
        ParquetAvroWriter::buildWriter,
        records.toArray(new GenericData.Record[] {}));

    InputFile inputFile = Files.localInput(file);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
      assertThat(reader.getRowGroups()).hasSize(2);
      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      assertThat(blocks).hasSize(2);

      Statistics<?> smallStatistics = getOnlyElement(blocks.get(0).getColumns()).getStatistics();
      assertThat(smallStatistics.hasNonNullValue()).isTrue();
      assertThat(smallStatistics.getMinBytes()).isEqualTo("test".getBytes(UTF_8));
      assertThat(smallStatistics.getMaxBytes()).isEqualTo("test".getBytes(UTF_8));

      // parquet-mr doesn't write stats larger than the max size rather than truncating
      Statistics<?> largeStatistics = getOnlyElement(blocks.get(1).getColumns()).getStatistics();
      assertThat(largeStatistics.hasNonNullValue()).isFalse();
      assertThat(largeStatistics.getMinBytes()).isNull();
      assertThat(largeStatistics.getMaxBytes()).isNull();
    }

    // Null count, lower and upper bounds should be empty because
    // one of the statistics in row groups is missing
    Metrics metrics = ParquetUtil.fileMetrics(inputFile, MetricsConfig.getDefault());
    assertThat(metrics.nullValueCounts()).isEmpty();
    assertThat(metrics.lowerBounds()).isEmpty();
    assertThat(metrics.upperBounds()).isEmpty();
  }

  @Test
  public void testNumberOfBytesWritten() throws IOException {
    Schema schema = new Schema(optional(1, "intCol", IntegerType.get()));

    // this value was specifically derived to reproduce iss1980
    // record count grow factor is 10000 (hardcoded)
    // total 10 checkSize method calls
    // for the 10th time (the last call of the checkSize method) nextCheckRecordCount == 100100
    // 100099 + 1 >= 100100
    int recordCount = 100099;
    File file = createTempFile(temp);

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(recordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= recordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    long actualSize =
        write(
            file,
            schema,
            Collections.emptyMap(),
            ParquetAvroWriter::buildWriter,
            records.toArray(new GenericData.Record[] {}));

    long expectedSize = ParquetIO.file(localInput(file)).getLength();
    assertThat(actualSize).isEqualTo(expectedSize);
  }

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = temp.toFile();
    assertThat(testFile.delete()).isTrue();

    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(testFile.toURI()))
            .withDataModel(GenericData.get())
            .withSchema(avroSchema)
            .config("parquet.avro.add-list-element-records", "true")
            .config("parquet.avro.write-old-list-structure", "true")
            .build();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    // Record 1: single element list, topbytes present
    byte[] b1 = {0x00, 0x01};
    ByteBuffer expectedBinary1 = ByteBuffer.wrap(b1);
    List<ByteBuffer> expectedByteList1 = Collections.singletonList(expectedBinary1);
    recordBuilder.set("arraybytes", expectedByteList1);
    recordBuilder.set("topbytes", expectedBinary1);
    writer.write(recordBuilder.build());

    // Record 2: empty list, topbytes null
    recordBuilder = new GenericRecordBuilder(avroSchema);
    recordBuilder.set("arraybytes", Collections.emptyList());
    recordBuilder.set("topbytes", null);
    writer.write(recordBuilder.build());

    // Record 3: multi-element list, different topbytes
    byte[] b2a = new byte[] {0x02, 0x03};
    byte[] b2b = new byte[] {0x04, 0x05};
    byte[] top = new byte[] {0x06, 0x07};
    ByteBuffer expectedBinary2 = ByteBuffer.wrap(top);
    List<ByteBuffer> expectedByteList2 = Arrays.asList(ByteBuffer.wrap(b2a), ByteBuffer.wrap(b2b));
    recordBuilder = new GenericRecordBuilder(avroSchema);
    recordBuilder.set("arraybytes", expectedByteList2);
    recordBuilder.set("topbytes", expectedBinary2);
    writer.write(recordBuilder.build());

    // Record 4: null list (arraybytes omitted), topbytes present
    recordBuilder = new GenericRecordBuilder(avroSchema);
    recordBuilder.set("arraybytes", null);
    recordBuilder.set("topbytes", expectedBinary1);
    writer.write(recordBuilder.build());

    writer.close();

    List<GenericData.Record> recordsRead =
        Lists.newArrayList(
            Parquet.read(Files.localInput(testFile)).project(schema).callInit().build());

    assertThat(recordsRead).hasSize(4);

    // Record 1 assertions
    GenericData.Record r1 = recordsRead.get(0);
    assertThat(r1.get("arraybytes")).isEqualTo(expectedByteList1);
    assertThat(r1.get("topbytes")).isEqualTo(expectedBinary1);

    // Record 2 assertions
    GenericData.Record r2 = recordsRead.get(1);
    assertThat((List) r2.get("arraybytes")).isEmpty();
    assertThat(r2.get("topbytes")).isNull();

    // Record 3 assertions
    GenericData.Record r3 = recordsRead.get(2);
    assertThat(r3.get("arraybytes")).isEqualTo(expectedByteList2);
    assertThat(r3.get("topbytes")).isEqualTo(expectedBinary2);

    // Record 4 assertions
    GenericData.Record r4 = recordsRead.get(3);
    assertThat(r4.get("arraybytes")).isNull();
    assertThat(r4.get("topbytes")).isEqualTo(expectedBinary1);
  }

  private Pair<File, Long> generateFile(
      Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
      int desiredRecordCount,
      Integer rowGroupSizeBytes,
      Integer minCheckRecordCount,
      Integer maxCheckRecordCount)
      throws IOException {
    Schema schema = new Schema(optional(1, "intCol", IntegerType.get()));

    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    if (rowGroupSizeBytes != null) {
      propsBuilder.put(PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(rowGroupSizeBytes));
    }
    if (minCheckRecordCount != null) {
      propsBuilder.put(
          PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, Integer.toString(minCheckRecordCount));
    }
    if (maxCheckRecordCount != null) {
      propsBuilder.put(
          PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, Integer.toString(maxCheckRecordCount));
    }

    List<GenericData.Record> records = Lists.newArrayListWithCapacity(desiredRecordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= desiredRecordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    File file = createTempFile(temp);
    long size =
        write(
            file,
            schema,
            propsBuilder.build(),
            createWriterFunc,
            records.toArray(new GenericData.Record[] {}));
    return Pair.of(file, size);
  }
}
