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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestSparkParquetReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    assumeThat(
            TypeUtil.find(
                writeSchema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()))
        .as("Parquet Avro cannot write non-string map keys")
        .isNull();

    List<GenericData.Record> expected = RandomData.generateList(writeSchema, 100, 0L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(testFile)).schema(writeSchema).named("test").build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader =
        Parquet.read(Files.localInput(testFile))
            .project(expectedSchema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(expectedSchema, type))
            .build()) {
      Iterator<InternalRow> rows = reader.iterator();
      for (GenericData.Record record : expected) {
        assertThat(rows).as("Should have expected number of rows").hasNext();
        assertEqualsUnsafe(expectedSchema.asStruct(), record, rows.next());
      }
      assertThat(rows).as("Should not have extra rows").isExhausted();
    }
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  protected List<InternalRow> rowsFromFile(InputFile inputFile, Schema schema) throws IOException {
    try (CloseableIterable<InternalRow> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(schema, type))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  protected Table tableFromInputFile(InputFile inputFile, Schema schema) throws IOException {
    HadoopTables tables = new HadoopTables();
    Table table =
        tables.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(),
            java.nio.file.Files.createTempDirectory(temp, null).toFile().getCanonicalPath());

    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withFormat(FileFormat.PARQUET)
                .withInputFile(inputFile)
                .withMetrics(ParquetUtil.fileMetrics(inputFile, MetricsConfig.getDefault()))
                .withFileSizeInBytes(inputFile.getLength())
                .build())
        .commit();

    return table;
  }

  @Test
  public void testInt96TimestampProducedBySparkIsReadCorrectly() throws IOException {
    String outputFilePath = String.format("%s/%s", temp.toAbsolutePath(), "parquet_int96.parquet");
    HadoopOutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(outputFilePath), new Configuration());
    Schema schema = new Schema(required(1, "ts", Types.TimestampType.withZone()));
    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField("ts", DataTypes.TimestampType, true, Metadata.empty())
            });
    List<InternalRow> rows = Lists.newArrayList(RandomData.generateSpark(schema, 10, 0L));

    try (ParquetWriter<InternalRow> writer =
        new NativeSparkWriterBuilder(outputFile)
            .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
            .set("spark.sql.parquet.writeLegacyFormat", "false")
            .set("spark.sql.parquet.outputTimestampType", "INT96")
            .set("spark.sql.parquet.fieldId.write.enabled", "true")
            .build()) {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    }

    InputFile parquetInputFile = Files.localInput(outputFilePath);
    List<InternalRow> readRows = rowsFromFile(parquetInputFile, schema);

    assertThat(readRows).hasSameSizeAs(rows);
    assertThat(readRows).isEqualTo(rows);

    // Now we try to import that file as an Iceberg table to make sure Iceberg can read
    // Int96 end to end.
    Table int96Table = tableFromInputFile(parquetInputFile, schema);
    List<Record> tableRecords = Lists.newArrayList(IcebergGenerics.read(int96Table).build());

    assertThat(tableRecords).hasSameSizeAs(rows);

    for (int i = 0; i < tableRecords.size(); i++) {
      GenericsHelpers.assertEqualsUnsafe(schema.asStruct(), tableRecords.get(i), rows.get(i));
    }
  }

  /**
   * Native Spark ParquetWriter.Builder implementation so that we can write timestamps using Spark's
   * native ParquetWriteSupport.
   */
  private static class NativeSparkWriterBuilder
      extends ParquetWriter.Builder<InternalRow, NativeSparkWriterBuilder> {
    private final Map<String, String> config = Maps.newHashMap();

    NativeSparkWriterBuilder(org.apache.parquet.io.OutputFile path) {
      super(path);
    }

    public NativeSparkWriterBuilder set(String property, String value) {
      this.config.put(property, value);
      return self();
    }

    @Override
    protected NativeSparkWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<InternalRow> getWriteSupport(Configuration configuration) {
      for (Map.Entry<String, String> entry : config.entrySet()) {
        configuration.set(entry.getKey(), entry.getValue());
      }

      return new org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport();
    }
  }

  @Test
  public void testMissingRequiredWithoutDefault() {
    Schema writeSchema = new Schema(required(1, "id", Types.LongType.get()));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.required("missing_str")
                .withId(6)
                .ofType(Types.StringType.get())
                .withDoc("Missing required field with no default")
                .build());

    assertThatThrownBy(() -> writeAndValidate(writeSchema, expectedSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required field: missing_str");
  }

  @Test
  public void testTwoLevelList() throws Exception {
    // Mirror parquet TestParquet#testTwoLevelList: four record patterns
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File parquetFile = File.createTempFile("spark-two-level-list", ".parquet", temp.toFile());
    assertThat(parquetFile.delete()).isTrue();

    // Bytes used across records
    byte[] b1 = new byte[] {0x00, 0x01};
    ByteBuffer expectedBinary1 = ByteBuffer.wrap(b1);
    List<ByteBuffer> expectedByteList1 = Collections.singletonList(expectedBinary1);

    byte[] b2a = new byte[] {0x02, 0x03};
    byte[] b2b = new byte[] {0x04, 0x05};
    byte[] top = new byte[] {0x06, 0x07};
    ByteBuffer expectedBinary2 = ByteBuffer.wrap(top);
    List<ByteBuffer> expectedByteList2 = Arrays.asList(ByteBuffer.wrap(b2a), ByteBuffer.wrap(b2b));

    List<GenericData.Record> expectedRecords = Lists.newArrayList();

    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(
                new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath()))
            .withSchema(avroSchema)
            .withDataModel(GenericData.get())
            .config("parquet.avro.add-list-element-records", "true")
            .config("parquet.avro.write-old-list-structure", "true")
            .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);

      // Record 1: single element list, topbytes present
      builder.set("arraybytes", expectedByteList1);
      builder.set("topbytes", expectedBinary1);
      GenericData.Record r1 = builder.build();
      writer.write(r1);
      expectedRecords.add(r1);

      // Record 2: empty list, topbytes null
      builder = new GenericRecordBuilder(avroSchema);
      builder.set("arraybytes", Collections.emptyList());
      builder.set("topbytes", null);
      GenericData.Record r2 = builder.build();
      writer.write(r2);
      expectedRecords.add(r2);

      // Record 3: multi-element list, different topbytes
      builder = new GenericRecordBuilder(avroSchema);
      builder.set("arraybytes", expectedByteList2);
      builder.set("topbytes", expectedBinary2);
      GenericData.Record r3 = builder.build();
      writer.write(r3);
      expectedRecords.add(r3);

      // Record 4: null list (arraybytes omitted), topbytes present
      builder = new GenericRecordBuilder(avroSchema);
      builder.set("arraybytes", null);
      builder.set("topbytes", expectedBinary1);
      GenericData.Record r4 = builder.build();
      writer.write(r4);
      expectedRecords.add(r4);
    }

    List<InternalRow> rows = rowsFromFile(Files.localInput(parquetFile), schema);
    assertThat(rows).hasSize(expectedRecords.size());

    for (int i = 0; i < expectedRecords.size(); i++) {
      assertEqualsUnsafe(schema.asStruct(), expectedRecords.get(i), rows.get(i));
    }
  }
}
