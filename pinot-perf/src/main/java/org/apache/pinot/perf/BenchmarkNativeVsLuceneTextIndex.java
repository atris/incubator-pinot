/**
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
package org.apache.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MINUTES)
@State(Scope.Benchmark)
public class BenchmarkNativeVsLuceneTextIndex {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_LUCENE = "testSegmentLucene";
  private static final String SEGMENT_NAME_NATIVE = "testSegmentNative";
  private static final String DOMAIN_NAMES_COL_LUCENE = "DOMAIN_NAMES_LUCENE";
  private static final String DOMAIN_NAMES_COL_NATIVE = "DOMAIN_NAMES_NATIVE";

  private IndexSegment _indexSegment;

  private IndexSegment _luceneSegment;
  private IndexSegment _nativeIndexSegment;

  final String _luceneQuery =
      "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'www.domain1%') LIMIT 5000000";
  final String _nativeQuery =
      "SELECT * FROM MyTable WHERE DOMAIN_NAMES_NATIVE CONTAINS 'www.domain1.*' LIMIT 5000000";
  @Param("1000000")
  int _numRows;
  @Param({"0", "1", "10", "100"})
  int _numBlocks;
  @Param({"native", "lucene"})
  String _fstType;

  private PlanMaker _planMaker;
  private QueryContext _luceneQueryContext;
  private QueryContext _nativeQueryContext;
  private QueryContext _queryContext;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    _planMaker = new InstancePlanMakerImplV2();
    _luceneQueryContext = QueryContextConverterUtils.getQueryContextFromSQL(_luceneQuery);
    _nativeQueryContext = QueryContextConverterUtils.getQueryContextFromSQL(_nativeQuery);
    FileUtils.deleteQuietly(INDEX_DIR);

    buildLuceneSegment();
    buildNativeTextIndexSegment();

    _luceneSegment = loadLuceneSegment();
    _nativeIndexSegment = loadNativeIndexSegment();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _luceneSegment.destroy();
    _nativeIndexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<String> getDomainNames() {
    return Arrays.asList("www.domain1.com", "www.domain1.co.ab", "www.domain1.co.bc", "www.domain1.co.cd",
        "www.sd.domain1.com", "www.sd.domain1.co.ab", "www.sd.domain1.co.bc", "www.sd.domain1.co.cd", "www.domain2.com",
        "www.domain2.co.ab", "www.domain2.co.bc", "www.domain2.co.cd", "www.sd.domain2.com", "www.sd.domain2.co.ab",
        "www.sd.domain2.co.bc", "www.sd.domain2.co.cd");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putField(DOMAIN_NAMES_COL_LUCENE, domain);
      row.putField(DOMAIN_NAMES_COL_NATIVE, domain);
      rows.add(row);
    }
    return rows;
  }

  private void buildLuceneSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_LUCENE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_LUCENE, DOMAIN_NAMES_COL_NATIVE))
        .setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_LUCENE, FieldSpec.DataType.STRING).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_LUCENE);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void buildNativeTextIndexSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;

    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_NATIVE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            propertiesMap));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_LUCENE)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_NATIVE, FieldSpec.DataType.STRING).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_NATIVE);
    config.setFSTIndexType(fstType);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private ImmutableSegment loadLuceneSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LUCENE), indexLoadingConfig);
  }

  private ImmutableSegment loadNativeIndexSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    Map<String, Map<String, String>> columnPropertiesParentMap = new HashMap<>();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    indexLoadingConfig.setFSTIndexType(fstType);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    columnPropertiesParentMap.put(DOMAIN_NAMES_COL_NATIVE, propertiesMap);
    indexLoadingConfig.setColumnProperties(columnPropertiesParentMap);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_NATIVE), indexLoadingConfig);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void query(Blackhole bh) {
    if (_fstType.equalsIgnoreCase("lucene")) {
      _indexSegment = _luceneSegment;
      _queryContext = _luceneQueryContext;
    } else if (_fstType.equalsIgnoreCase("native")) {
      _indexSegment = _nativeIndexSegment;
      _queryContext = _nativeQueryContext;
    } else {
      throw new IllegalStateException("Unknown value seen");
    }

    Operator<?> operator = _planMaker.makeSegmentPlanNode(_indexSegment, _queryContext).run();
    bh.consume(operator);
    for (int i = 0; i < _numBlocks; i++) {
      bh.consume(operator.nextBlock());
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkNativeVsLuceneTextIndex.class.getSimpleName()).build()).run();
  }
}
