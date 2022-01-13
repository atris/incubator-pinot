/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import javax.annotation.Nullable;


/**
 * Client interface for an FST abstracting either the mutable or immutable FST
 * @author Atri Sharma
 */
public interface MutableFST {

  /**
   * The start state in the FST; there must be exactly one
   * @return
   */
  State getStartState();

  /**
   * Set the start state
   */
  void setStartState(MutableState mutableState);

  /**
   * throws an exception if the FST is constructed in an invalid state
   */
  void throwIfInvalid();

  /**
   * Add a path to the FST
   */
  void addPath(String word, int outputSymbol);
}
