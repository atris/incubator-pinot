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

import org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.FstUtils;


/**
 * A mutable FST's arc implementation.
 */
public class MutableArc implements Arc {

  private String outputSymbol;
  private MutableState nextState;

  /**
   * Arc Constructor
   *
   * @param nextState the arc's next state
   */
  public MutableArc(String outputSymbol, MutableState nextState) {
    this.outputSymbol = outputSymbol;
    this.nextState = nextState;
  }

  @Override
  public String getOutputSymbol() {
    return outputSymbol;
  }

  /**
   * Get the next state
   */
  @Override
  public MutableState getNextState() {
    return nextState;
  }

  /**
   * Set the next state
   *
   * @param nextState the next state to set
   */
  public void setNextState(MutableState nextState) {
    this.nextState = nextState;
  }

  @Override
  public boolean equals(Object obj) {
    return FstUtils.arcEquals(this, obj);
  }

  @Override
  public int hashCode() {
    int result = 1;

    result = 31 * result + (nextState != null ? nextState.getId() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "(" + nextState
           + ")";
  }
}
