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
package org.apache.pinot.core.operator.blocks;

import java.util.List;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;

/**
 * Represents a combination of multiple TransformBlock instances
 */
public class CombinedTransformBlock extends TransformBlock {
  protected List<TransformBlock> _transformBlockList;
  protected TransformBlock _nonFilteredAggBlock;

  public CombinedTransformBlock(List<TransformBlock> transformBlockList,
      TransformBlock nonFilteredAggBlock) {
    super(nonFilteredAggBlock == null ? null : nonFilteredAggBlock._projectionBlock,
        nonFilteredAggBlock == null ? null : nonFilteredAggBlock._transformFunctionMap);

    _transformBlockList = transformBlockList;
    _nonFilteredAggBlock = nonFilteredAggBlock;
  }

  public int getNumDocs() {
    int numDocs = 0;

    for (TransformBlock transformBlock : _transformBlockList) {
      if (transformBlock != null) {
        numDocs = numDocs + transformBlock._projectionBlock.getNumDocs();
      }
    }

    if (_nonFilteredAggBlock != null) {
      numDocs = numDocs + _nonFilteredAggBlock.getNumDocs();
    }

    return numDocs;
  }

  public List<TransformBlock> getTransformBlockList() {
    return _transformBlockList;
  }

  public TransformBlock getNonFilteredAggBlock() {
    return _nonFilteredAggBlock;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }
}
