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
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/**
 * The iterator performs a linear pass through the underlying child iterator and returns
 * the complement of the result set.
 */
public class NotDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator _childDocIdIterator;
  private final int _numDocs;
  private int _nextDocId;
  private int _nextNonMatchingDocId;

  public NotDocIdIterator(BlockDocIdIterator childDocIdIterator, int numDocs) {
    _childDocIdIterator = childDocIdIterator;
    _nextDocId = 0;

    int currentDocIdFromChildIterator = childDocIdIterator.next();
    _nextNonMatchingDocId = currentDocIdFromChildIterator == Constants.EOF ? numDocs : currentDocIdFromChildIterator;
    _numDocs = numDocs;
  }

  @Override
  public int next() {
    while (_nextDocId == _nextNonMatchingDocId) {
      _nextDocId = _nextNonMatchingDocId + 1;

      int nextMatchingDocId = _childDocIdIterator.next();

      if (nextMatchingDocId == Constants.EOF) {
        _nextNonMatchingDocId = _numDocs;
      } else {
        _nextNonMatchingDocId = nextMatchingDocId;
      }
    }

    if (_nextDocId >= _numDocs) {
      return Constants.EOF;
    }

    return _nextDocId++;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId == Constants.EOF || targetDocId > _numDocs) {
      return Constants.EOF;
    }

    if (targetDocId < _nextDocId) {
      return _nextDocId;
    }

    _nextDocId = targetDocId + 1;

    int upperLimit = findUpperLimitGreaterThanDocId(targetDocId);

    if (upperLimit == Constants.EOF) {
      _nextNonMatchingDocId = _numDocs;
    } else {
      _nextNonMatchingDocId = upperLimit;
    }

    return _nextDocId++;
  }

  private int findUpperLimitGreaterThanDocId(int currentDocId) {
    int result = _childDocIdIterator.advance(currentDocId);

    while (result <= currentDocId && result != Constants.EOF) {
      result = _childDocIdIterator.next();
    }

    return result;
  }
}
