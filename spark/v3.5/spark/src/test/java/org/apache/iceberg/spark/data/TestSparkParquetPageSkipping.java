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

import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroWriter;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkParquetPageSkipping {

  private static final Types.StructType PRIMITIVES =
      Types.StructType.of(
          required(0, "_long", Types.LongType.get()),
          optional(1, "_string", Types.StringType.get()), // var width
          required(2, "_bool", Types.BooleanType.get()),
          optional(3, "_int", Types.IntegerType.get()),
          optional(4, "_float", Types.FloatType.get()),
          required(5, "_double", Types.DoubleType.get()),
          optional(6, "_date", Types.DateType.get()),
          required(7, "_ts", Types.TimestampType.withZone()),
          required(8, "_fixed", Types.FixedType.ofLength(7)),
          optional(9, "_bytes", Types.BinaryType.get()), // var width
          required(10, "_dec_9_0", Types.DecimalType.of(9, 0)), // int
          required(11, "_dec_11_2", Types.DecimalType.of(11, 2)), // long
          required(12, "_dec_38_10", Types.DecimalType.of(38, 10)) // fixed
          );

  private static final Schema PRIMITIVES_SCHEMA = new Schema(PRIMITIVES.fields());

  private static final Types.StructType LIST =
      Types.StructType.of(
          optional(13, "_list", Types.ListType.ofOptional(14, Types.StringType.get())));
  private static final Types.StructType MAP =
      Types.StructType.of(
          optional(
              15,
              "_map",
              Types.MapType.ofOptional(16, 17, Types.StringType.get(), Types.StringType.get())));
  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          Lists.newArrayList(Iterables.concat(PRIMITIVES.fields(), LIST.fields(), MAP.fields())));

  @TempDir File tempDir;

  private File testFile;
  private List<GenericData.Record> allRecords = Lists.newArrayList();
  private List<GenericData.Record> rowGroup0;
  private List<GenericData.Record> rowGroup1;

  /* Column and offset indexes info of `_long` column in `testFile` copied from text printed by parquet-cli's
  column-index command:

  row-group 0:
  column index for column _long:
  Boundary order: ASCENDING
                        null count  min                                       max
  page-0                         0  0                                         56
  page-1                         0  57                                        113
  page-2                         0  114                                       170
  page-3                         0  171                                       227
  page-4                         0  228                                       284
  page-5                         0  285                                       341
  page-6                         0  342                                       398
  page-7                         0  399                                       455
  page-8                         0  456                                       512
  page-9                         0  513                                       569
  page-10                        0  570                                       592

  offset index for column _long:
                            offset   compressed size       first row index
  page-0                         4               137                     0
  page-1                       141               138                    57
  page-2                       279               137                   114
  page-3                       416               137                   171
  page-4                       553               137                   228
  page-5                       690               141                   285
  page-6                       831               140                   342
  page-7                       971               141                   399
  page-8                      1112               141                   456
  page-9                      1253               140                   513
  page-10                     1393                93                   570

  column index for column _string:
  Boundary order: UNORDERED
                        null count  min                                       max
  page-0                         1                                            x-n
  page-1                         0  .KjGZWrjN!YeP2p1a25-SN1u                  wywLEoFOHBJ2
  page-2                         1  !XxGN10jhWJ5kgqAP(...)G83HT.OEiaxUpUHfqC  y6BcJtFYJx0NUItlhe?ai6HG_rSQxt
  page-3                         0  0                                         yj!0f
  page-4                         1  !NW2pxFIaLmcU6MFNtJ1NOfoszz               zuksdiwHNcjyiG0IE(...)2NjaAxQ?KC8D!L7u9Q
  page-5                         0  0Icx                                      xyCLIoIn-Vme7MlJQP98TpPAOoBfis-oj
  page-6                         0                                            wqOo.f_Iwd6uyJ0Gl8ZB5AusJ
  page-7                         0                                            wSeh!MU4
  page-8                         1                                            s0Rp!!_LC.VDcr1JeUZ_.TV9yJV5aHEAvA
  page-9                         1                                            wb8mpWjsryYg467Z7
  page-10                        1  5!Z-52jKsWIF.78ZY(...)qKO!pU4oktmC5Mkm__  tlp
  page-11                        1  15qRH1TtLoqiDYGYN(...)IrN_-v!.SLsQl0p!wf  uU!f?kM.yJUCGF.4K
  page-12                        0                                            szqvtVHWaDtt0FwQP?0Fmm1_Hc
  page-13                        1                                            xDnaaY.G1EDkiT
  page-14                        0  66na-7g46NJO783S6xcW-uqunWR5vOmx          zJXjs0Yv-!vI7yFoKBL4IqV6oy
  page-15                        2  -849YlS.gJT?                              zWgCICxC70AqIM!Of(...)FAtctS4A6F1JNRzUXU
  page-16                        2  2?ZMhVJ6IEMxSuJWNq.uEBv4yyys8             sioBeCz8XVVIiqnAXYXiFYk2FoNVnrpVC?
  page-17                        1  9                                         yxIE682XL7wiPPB93z
  page-18                        1  2G8rJO1ftOSxmrP05qnWG                     z2MDTvdWcc4S-8Xf0h9xTzljHwfRw-f0pA6
  page-19                        0                                            zkaY2ROqd?_gC69FikNsJE5B86m6uvUkQW73doY
  page-20                        2  !                                         y3AvFG
  page-21                        0  0kQhHVM148fiVZ.?4(...)N-ytH0jksLsszimqaj  yHN.lflk?K_hckbb6R!lJOEj-G!wLo4l
  page-22                        0  -Dm670q3-sARAHhpj(...)utrGuU3So_dBtAz4RD  ywI8WQ?8A?7kgf2DEbejVr
  page-23                        2  !U.L.WkPj9jtd9l39z!CsYK3XUpq?v            yiv.bHNWX1e2LmzRbj-
  page-24                        0  -LQjqpDqWUmmINBVh(...)-xjFfze1fZr-Htf4gG  zH5qpwGk20lQhN6QjT9ZhGsPbe0pI?BDIg1u.S_c
  page-25                        1  !bF.sS6zzDfYtZrO                          oqq!yE3weaT_-SP2AKKyMgY4T!-
  page-26                        0  0iSfGOOsSiTd                              yEA4OgFErS9ofS1gyUFajQmctWLicXEm
  page-27                        2  !Yue8d                                    sAVL.wFl4fD9gZ-3mpL!nukXlv
  page-28                        0                                            w5hzORO2QZ88mhNooqtSh0r
  page-29                        0                                            z?jo0lChhkdjsvFQaewnqeJe
  page-30                        1  .7oD8HBBJsi                               xuSNBEtl5iGAzZIHw(...)-HCk6h4SX0puth!DE-
  page-31                        1  1Ss5Hjn7FdWM?heQs(...)oi1uvhtk0iTUaWwTkV  xVs0jMmE?xfhVl?Cu
  page-32                        0  !5Wsu0ijcCPsx.z!GuGlm.DYh                 y61j1kQ
  page-33                        0  5AN4Ak                                    vU7ZoU!OoyEIzJMx

  offset index for column _string:
                            offset   compressed size       first row index
  page-0                      1486               447                     0
  page-1                      1933               446                    21
  page-2                      2379               454                    38
  page-3                      2833               451                    53
  page-4                      3284               444                    78
  page-5                      3728               453                    92
  page-6                      4181               445                   110
  page-7                      4626               442                   127
  page-8                      5068               439                   146
  page-9                      5507               459                   165
  page-10                     5966               459                   182
  page-11                     6425               443                   198
  page-12                     6868               439                   217
  page-13                     7307               453                   235
  page-14                     7760               439                   256
  page-15                     8199               439                   270
  page-16                     8638               456                   293
  page-17                     9094               446                   308
  page-18                     9540               468                   326
  page-19                    10008               442                   343
  page-20                    10450               438                   360
  page-21                    10888               451                   378
  page-22                    11339               446                   394
  page-23                    11785               454                   407
  page-24                    12239               442                   425
  page-25                    12681               447                   440
  page-26                    13128               457                   456
  page-27                    13585               438                   470
  page-28                    14023               450                   492
  page-29                    14473               449                   508
  page-30                    14922               463                   526
  page-31                    15385               451                   547
  page-32                    15836               446                   562
  page-33                    16282               330                   579

  row-group 1:
  column index for column _long:
  Boundary order: ASCENDING
                        null count  min                                       max
  page-0                         0  593                                       649
  page-1                         0  650                                       706
  page-2                         0  707                                       763
  page-3                         0  764                                       820
  page-4                         0  821                                       877
  page-5                         0  878                                       934
  page-6                         0  935                                       991
  page-7                         0  992                                       999

  offset index for column _long:
                            offset   compressed size       first row index
  page-0                    498685               140                     0
  page-1                    498825               140                    57
  page-2                    498965               141                   114
  page-3                    499106               140                   171
  page-4                    499246               141                   228
  page-5                    499387               140                   285
  page-6                    499527               142                   342
  page-7                    499669                68                   399

  column index for column _string:
  Boundary order: UNORDERED
                        null count  min                                       max
  page-0                         0  1BW                                       zo5aNke8yIy9UEwd60ehi_Lhe6wISX.7n
  page-1                         0  2kcdhDuRL0Z46!zwp(...)LK2GpbAXN0.oRHABMh  zzBYX?HiWMlMn
  page-2                         0  6i_YAvGwsfQuQJNB.hb8P                     yoYLC?n76Mxofz?zcEEI.xWImDE
  page-3                         1  5ONqWjvTfaUymH4on(...)FnIhmin7SIjgwyf472  rQl5rw
  page-4                         0  .PZoG-                                    z71En2ihh-QdZPv.fmC
  page-5                         0  -uSTM6mMLN6qsYLyR5O                       o5Q-U?e!CYGv7OU_dd-dtY.P?UyANwZp?qkJxdIs
  page-6                         0                                            zrUjt9c8TKodv
  page-7                         2                                            sn_4ZP9jFBhf7SZdw(...)Y54!E43P1?Hlza4!IG
  page-8                         1  5_aU7ErctFmocGdp8nFqo                     vrbSxJsQFmDNDBqFL.jpxY!5DaOn6tnwEcDdA
  page-9                         0  2?jZEj!YR6Nne-os5ksGR9-TeB2k?uLk4gz_5701  vvvDZNzSa9W2LQ74
  page-10                        1  -ECRjZCNzaS3Gxtlh                         xgrC!m?QXL6r7Cyy-(...)erJYzc7-U8bh3OLPLv
  page-11                        1                                            vu6YNypKHkz9qWzGQ(...)dvqqQrxfKX4f-in3XN
  page-12                        0  -Gg9iy40zYEsX0yPxCiC5A?k                  zO-Ke4e6!i7Q!XgAvAfOuE-wyc.iblpOb3Ynil
  page-13                        0  !vAx2U3nRK92tx.hh(...)BfUL!mzxmPM4w84y_G  wwlB9qzeqCp0vcfHw2eM7D8
  page-14                        1  03pZyZ_8G-Bg                              ubOtijWA6Y
  page-15                        2  3I--nb7zKiFr_DyXD(...)dK7v3X5?F_Evl7!lZo  x.EtsFMSkyYSypCCHe
  page-16                        0  -6Jo!QZt.rDn                              oFlmp9R19y!c0U
  page-17                        0  3Ix5RcBS.sp.2HCen5S8Wup                   ySrww
  page-18                        1  !6SHwzBO8?luTb..dcT_                      vLEhekyQZEN3lkA8gjTbPQMqvH1411AJidK
  page-19                        0  5eKesD5M-xCWUMYAb(...)?5iKSeIkHUDfe92fv.  rB-KILLY
  page-20                        1  !XXhAf1yt8XE                              yIHn8IJSQJa2UQRkhkF3lZq-b6!xkB.C6t
  page-21                        0  .Y                                        yj
  page-22                        0                                            zv
  page-23                        1  2o2QVjDHyqlzqszj??SL                      w4d9fbcaA8_yfIZ
  page-24                        0  4fzBy3irU_Oe8egRUtfiBq?Vo                 4pKs5IM!v6pa-XWUDuI8vDL

  offset index for column _string:
                            offset   compressed size       first row index
  page-0                    499737               440                     0
  page-1                    500177               431                    15
  page-2                    500608               462                    34
  page-3                    501070               437                    48
  page-4                    501507               444                    62
  page-5                    501951               445                    79
  page-6                    502396               471                    94
  page-7                    502867               458                   113
  page-8                    503325               449                   134
  page-9                    503774               444                   151
  page-10                   504218               445                   167
  page-11                   504663               459                   182
  page-12                   505122               436                   203
  page-13                   505558               438                   220
  page-14                   505996               461                   234
  page-15                   506457               474                   251
  page-16                   506931               443                   269
  page-17                   507374               466                   284
  page-18                   507840               446                   301
  page-19                   508286               436                   316
  page-20                   508722               458                   330
  page-21                   509180               438                   347
  page-22                   509618               436                   368
  page-23                   510054               441                   388
  page-24                   510495               102                   405
  */

  private long index = -1;
  private static final int ABOVE_INT_COL_MAX_VALUE = Integer.MAX_VALUE;

  @BeforeEach
  public void generateFile() throws IOException {
    testFile = new File(tempDir, "pages_unaligned_file.parquet");
    if (testFile.exists()) {
      Assertions.assertTrue(testFile.delete(), "Delete should succeed");
    }

    Function<GenericData.Record, GenericData.Record> transform =
        record -> {
          index += 1;
          if (record.get("_long") != null) {
            record.put("_long", index);
          }

          if (Objects.equals(record.get("_int"), ABOVE_INT_COL_MAX_VALUE)) {
            record.put("_int", ABOVE_INT_COL_MAX_VALUE - 1);
          }

          return record;
        };

    int numRecords = 1000;
    allRecords =
        RandomData.generateList(COMPLEX_SCHEMA, numRecords, 0).stream()
            .map(transform)
            .collect(Collectors.toList());
    rowGroup0 = selectRecords(allRecords, Pair.of(0, 593));
    rowGroup1 = selectRecords(allRecords, Pair.of(593, 1000));

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(testFile))
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .schema(COMPLEX_SCHEMA)
            .set(PARQUET_PAGE_SIZE_BYTES, "500")
            .set(PARQUET_ROW_GROUP_SIZE_BYTES, "500000") // 2 row groups
            .set(PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .set(PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
            .set(PARQUET_DICT_SIZE_BYTES, "1")
            .named("pages_unaligned_file")
            .build()) {
      writer.addAll(allRecords);
    }
  }

  @Parameters(name = "vectorized = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{false}, {true}};
  }

  @Parameter(index = 0)
  private boolean vectorized;

  @TestTemplate
  public void testSinglePageMatch() {
    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("_long", 57),
            Expressions.lessThan("_long", 114)); // exactly page-1  -> row ranges: [57, 113]

    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(57, 114));
    readAndValidate(filter, expected, rowGroup0);
  }

  @TestTemplate
  public void testSinglePageMatchExpressionIn() {
    Expression filter = Expressions.in("_long", 80, 90, 100);

    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(57, 114));
    readAndValidate(filter, expected, rowGroup0);
  }

  @TestTemplate
  public void testSinglePageMatchUnorderedColumn() {
    // row-group 1
    // page-1                         0  2kcdhDuRL0Z46!zwp(...)LK2GpbAXN0.oRHABMh  zzBYX?HiWMlMn
    Expression filter = Expressions.equal("_string", "zz");

    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(608, 627));
    readAndValidate(filter, expected, rowGroup1);
  }

  @TestTemplate
  public void testSinglePageMatchUnorderedColumnExpressionIn() {
    // row-group 1
    // page-1                         0  2kcdhDuRL0Z46!zwp(...)LK2GpbAXN0.oRHABMh  zzBYX?HiWMlMn
    Expression filter = Expressions.in("_string", "zz", "zzz");

    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(608, 627));
    readAndValidate(filter, expected, rowGroup1);
  }

  @TestTemplate
  public void testMultiplePagesMatch() {
    Expression filter =
        Expressions.or(
            // page-1  -> row ranges: [57, 113]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 57), Expressions.lessThan("_long", 114)),

            // page-3, page-4 in row group 0  -> row ranges[171, 284]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 173), Expressions.lessThan("_long", 260)));

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(171, 285));
    readAndValidate(filter, expected, rowGroup0);
  }

  @TestTemplate
  public void testMultiplePagesMatchExpressionIn() {
    // Looks like in predicate for the sorted long column is somehow identical to `min <= x <= max`?
    Expression filter = Expressions.in("_long", 80, 100, 180, 200);

    // page-1  -> row ranges: [57, 113]
    // page-3  -> row ranges: [171, 227]
    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(57, 228));
    readAndValidate(filter, expected, rowGroup0);
  }

  @TestTemplate
  public void testMultiplePagesMatchUnorderedColumn() {
    // row-group 1
    // page-1                         0  2kcdhDuRL0Z46!zwp(...)LK2GpbAXN0.oRHABMh  zzBYX?HiWMlMn
    // page-22                        0                                            zv
    Expression filter = Expressions.equal("_string", "zv");

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(608, 627), Pair.of(961, 981));
    readAndValidate(filter, expected, rowGroup1);
  }

  @TestTemplate
  public void testMultiplePagesMatchUnorderedColumnExpressionIn() {
    // row-group 1
    // page-1                         0  2kcdhDuRL0Z46!zwp(...)LK2GpbAXN0.oRHABMh  zzBYX?HiWMlMn
    // page-22                        0                                            zv
    Expression filter = Expressions.in("_string", "zv", "zz", "zzz");

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(608, 627), Pair.of(961, 981));
    readAndValidate(filter, expected, rowGroup1);
  }

  @TestTemplate
  public void testMultipleRowGroupsMatch() {
    Expression filter =
        Expressions.or(
            // page-1  -> row ranges: [57, 113]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 57), Expressions.lessThan("_long", 114)),

            // page-3, page-4 in row group 0  -> row ranges[171, 284]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 173), Expressions.lessThan("_long", 260)));

    filter =
        Expressions.or(
            filter,
            // page-10 in row group 0 and page-0, page-1 in row group 1 -> row ranges: [570, 706]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 572), Expressions.lessThan("_long", 663)));

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(171, 285), Pair.of(570, 707));
    readAndValidate(filter, expected, allRecords);
  }

  @TestTemplate
  public void testOnlyFilterPagesOnOneRowGroup() {
    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("_long", 57),
            Expressions.lessThan("_long", 114)); // exactly page-1  -> row ranges: [57, 113]

    filter =
        Expressions.or(
            filter,
            // page-9, page-10 in row group 0 -> row ranges: [513, 592]
            // and all pages in row group 1
            Expressions.greaterThanOrEqual("_long", 569));

    // some pages of row group 0 and all pages of row group 1
    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(513, 593), Pair.of(593, 1000));

    readAndValidate(filter, expected, allRecords);
  }

  @TestTemplate
  public void testNoRowsMatch() {
    Expression filter =
        Expressions.and(
            Expressions.and(
                Expressions.greaterThan("_long", 40), Expressions.lessThan("_long", 46)),
            Expressions.equal("_int", ABOVE_INT_COL_MAX_VALUE));

    readAndValidate(filter, ImmutableList.of(), ImmutableList.of());
  }

  @TestTemplate
  public void testAllRowsMatch() {
    Expression filter = Expressions.greaterThanOrEqual("_long", Long.MIN_VALUE);
    readAndValidate(filter, allRecords, allRecords);
  }

  private Schema readSchema() {
    return vectorized ? PRIMITIVES_SCHEMA : COMPLEX_SCHEMA;
  }

  private void readAndValidate(
      Expression filter,
      List<GenericData.Record> expected,
      List<GenericData.Record> vectorizedExpected) {
    Schema projected = readSchema();

    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(projected).filter(filter);

    Types.StructType struct = projected.asStruct();

    if (vectorized) {
      CloseableIterable<ColumnarBatch> batches =
          builder
              .createBatchedReaderFunc(
                  type ->
                      VectorizedSparkParquetReaders.buildReader(
                          projected, type, ImmutableMap.of(), null))
              .build();

      Iterator<GenericData.Record> expectedIterator = vectorizedExpected.iterator();
      for (ColumnarBatch batch : batches) {
        TestHelpers.assertEqualsBatch(struct, expectedIterator, batch);
      }

      Assertions.assertFalse(
          expectedIterator.hasNext(), "The expected records is more than the actual result");
    } else {
      CloseableIterable<InternalRow> reader =
          builder
              .createReaderFunc(type -> SparkParquetReaders.buildReader(projected, type))
              .build();
      CloseableIterator<InternalRow> actualRows = reader.iterator();

      for (GenericData.Record record : expected) {
        Assertions.assertTrue(actualRows.hasNext(), "Should have expected number of rows");
        InternalRow row = actualRows.next();
        TestHelpers.assertEqualsUnsafe(struct, record, row);
      }

      Assertions.assertFalse(actualRows.hasNext(), "Should not have extra rows");
    }
  }

  @SafeVarargs
  private static List<GenericData.Record> selectRecords(
      List<GenericData.Record> records, Pair<Integer, Integer>... ranges) {
    return Arrays.stream(ranges)
        .map(range -> records.subList(range.first(), range.second()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
