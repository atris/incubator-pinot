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
package org.apache.pinot.segment.local.utils.nativefst.builders;

import com.carrotsearch.hppc.IntIntHashMap;
import java.util.BitSet;
import org.apache.pinot.segment.local.utils.nativefst.FSA;


/**
 * Compute additional information about an FSA: number of arcs, nodes, etc.
 */
public final class FSAInfo {
  /**
   * Computes the exact number of states and nodes by recursively traversing the FSA.
   */
  private static class NodeVisitor {
    final BitSet _visitedArcs = new BitSet();
    final BitSet _visitedNodes = new BitSet();

    int _nodes;
    int _arcs;
    int _totalArcs;

    private final FSA _fsa;

    NodeVisitor(FSA fsa) {
      this._fsa = fsa;
    }

    public void visitNode(final int node) {
      if (_visitedNodes.get(node)) {
        return;
      }
      _visitedNodes.set(node);

      _nodes++;
      for (int arc = _fsa.getFirstArc(node); arc != 0; arc = _fsa.getNextArc(arc)) {
        if (!_visitedArcs.get(arc)) {
          _arcs++;
        }
        _totalArcs++;
        _visitedArcs.set(arc);

        if (!_fsa.isArcTerminal(arc)) {
          visitNode(_fsa.getEndNode(arc));
        }
      }
    }
  }

  /**
   * Computes the exact number of final states.
   */
  private static class FinalStateVisitor {
    final IntIntHashMap visitedNodes = new IntIntHashMap();

    private final FSA fsa;

    FinalStateVisitor(FSA fsa) {
      this.fsa = fsa;
    }

    public int visitNode(int node) {
      int index = visitedNodes.indexOf(node);
      if (index >= 0) {
        return visitedNodes.indexGet(index);
      }

      int fromHere = 0;
      for (int arc = fsa.getFirstArc(node); arc != 0; arc = fsa.getNextArc(arc)) {
        if (fsa.isArcFinal(arc))
          fromHere++;

        if (!fsa.isArcTerminal(arc)) {
          fromHere += visitNode(fsa.getEndNode(arc));
        }
      }
      visitedNodes.put(node, fromHere);
      return fromHere;
    }
  }

  /**
   * Number of nodes in the automaton.
   */
  public final int nodeCount;

  /**
   * Number of arcs in the automaton, excluding an arcs from the zero node (initial) and an arc from the start node to
   * the root node.
   */
  public final int arcsCount;

  /**
   * Total number of arcs, counting arcs that physically overlap due to merging.
   */
  public final int arcsCountTotal;

  /**
   * Number of final states (number of input sequences stored in the automaton).
   */
  public final int finalStatesCount;

  /*
	 * 
	 */
  public FSAInfo(FSA fsa) {
    final NodeVisitor w = new NodeVisitor(fsa);
    int root = fsa.getRootNode();
    if (root > 0) {
      w.visitNode(root);
    }

    this.nodeCount = 1 + w._nodes;
    this.arcsCount = 1 + w._arcs;
    this.arcsCountTotal = 1 + w._totalArcs;

    final FinalStateVisitor fsv = new FinalStateVisitor(fsa);
    this.finalStatesCount = fsv.visitNode(fsa.getRootNode());
  }

  /*
	 * 
	 */
  public FSAInfo(int nodeCount, int arcsCount, int arcsCountTotal, int finalStatesCount) {
    this.nodeCount = nodeCount;
    this.arcsCount = arcsCount;
    this.arcsCountTotal = arcsCountTotal;
    this.finalStatesCount = finalStatesCount;
  }

  /*
	 * 
	 */
  @Override
  public String toString() {
    return "Nodes: " + nodeCount + ", arcs visited: " + arcsCount + ", arcs total: " + arcsCountTotal
        + ", final states: " + finalStatesCount;
  }
}
