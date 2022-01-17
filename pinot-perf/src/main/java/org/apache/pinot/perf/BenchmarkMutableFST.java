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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFSTImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkMutableFST {
  @Param({"q.[aeiou]c.*", ".*a", "b.*", ".*", ".*ated", ".*ba.*"})
  public String _regex;

  private MutableFST _mutableFST;
  private org.apache.lucene.util.fst.FST _fst;

  @Setup
  public void setUp()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();

    _mutableFST = new MutableFSTImpl();
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("data/words.txt"))))) {
      String currentWord;
      int i = 0;
      while ((currentWord = bufferedReader.readLine()) != null) {
        _mutableFST.addPath(currentWord, i);
        input.put(currentWord, i++);
      }
    }

    _fst = buildFST(input);
  }

  @Benchmark
  public MutableRoaringBitmap testMutableRegex() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch(_regex, _mutableFST, writer::add);

    return writer.get();
  }

  @Benchmark
  public List testLuceneRegex()
      throws IOException {
    return org.apache.pinot.segment.local.utils.fst.RegexpMatcher.regexMatch(_regex, _fst);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkFST.class.getSimpleName()).build()).run();
  }
}
