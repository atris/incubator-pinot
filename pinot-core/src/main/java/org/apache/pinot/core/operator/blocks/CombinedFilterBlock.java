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
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;


public class CombinedFilterBlock extends FilterBlock {
  private final List<FilterBlock> _filterBlockList;

  public CombinedFilterBlock(List<FilterBlock> filterBlockList) {
    // Seed with the first value. This has no relevance
    super(filterBlockList.get(0).getBlockDocIdSet());

    _filterBlockList = filterBlockList;
  }

  public List<FilterBlock> getFilterBlockList() {
    return _filterBlockList;
  }

  @Override
  public FilterBlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }
}
