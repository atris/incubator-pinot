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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.CombinedFilterOperator;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.transform.CombinedTransformOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.FilterableAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.plan.StarTreeTransformPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


/**
 * The <code>AggregationGroupByPlanNode</code> class provides the execution plan for aggregation group-by query on a
 * single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationGroupByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public AggregationGroupByPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public AggregationGroupByOperator run() {
    assert _queryContext.getAggregationFunctions() != null;
    assert _queryContext.getGroupByExpressions() != null;

    ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
    boolean hasFilteredPredicates = _queryContext.isHasFilteredAggregations();

    Pair<FilterPlanNode, BaseFilterOperator> filterOperatorPair =
        buildFilterOperator(_queryContext.getFilter());

    Pair<TransformOperator,
        AggregationGroupByOperator> pair =
        buildOperators(filterOperatorPair.getRight(), filterOperatorPair.getLeft(), false,
            null);

    if (hasFilteredPredicates) {
      int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
      AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();

      Set<ExpressionContext> expressionsToTransform =
          AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressions);
      BaseOperator<IntermediateResultsBlock> aggregationOperator = pair.getRight();

      assert pair != null && aggregationOperator != null;

      //TODO: For non star tree, non filtered aggregation, share the main filter transform operator
      TransformOperator filteredTransformOperator = buildOperatorForFilteredAggregations(pair.getLeft(),
          filterOperatorPair.getRight(), expressionsToTransform, aggregationFunctions);
      return new AggregationGroupByOperator(_queryContext, groupByExpressions,
          filteredTransformOperator, numTotalDocs, false);
    }

    return pair.getRight();
  }

  private TransformOperator buildOperatorForFilteredAggregations(TransformOperator nonFilteredTransformOperator,
      BaseFilterOperator filterOperator,
      Set<ExpressionContext> expressionsToTransform,
      AggregationFunction[] aggregationFunctions) {
    Map<ExpressionContext, TransformOperator> transformOperatorList = new HashMap<>();
    Map<ExpressionContext, BaseFilterOperator> baseFilterOperatorMap =
        new HashMap<>();
    List<Pair<ExpressionContext, Pair<FilterPlanNode, BaseFilterOperator>>> filterPredicatesAndMetadata =
        new ArrayList<>();

    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      if (aggregationFunction instanceof FilterableAggregationFunction) {
        FilterableAggregationFunction filterableAggregationFunction =
            (FilterableAggregationFunction) aggregationFunction;
        Pair<FilterPlanNode, BaseFilterOperator> pair =
            buildFilterOperator(filterableAggregationFunction.getFilterContext());

        baseFilterOperatorMap.put(filterableAggregationFunction.getAssociatedExpressionContext(),
            pair.getRight());
        filterPredicatesAndMetadata.add(Pair.of(filterableAggregationFunction.getAssociatedExpressionContext(),
            pair));
      }
    }

    CombinedFilterOperator combinedFilterOperator = new CombinedFilterOperator(baseFilterOperatorMap,
        filterOperator);

    for (Pair<ExpressionContext, Pair<FilterPlanNode, BaseFilterOperator>> pair :
        filterPredicatesAndMetadata) {
      Pair<TransformOperator,
          AggregationGroupByOperator> innerPair =
          buildOperators(combinedFilterOperator, pair.getRight().getLeft(),
              true, pair.getLeft());

      transformOperatorList.put(pair.getLeft(), innerPair.getLeft());
    }

    return new CombinedTransformOperator(transformOperatorList, nonFilteredTransformOperator,
        filterOperator, expressionsToTransform);
  }

  private Pair<FilterPlanNode, BaseFilterOperator> buildFilterOperator(FilterContext filterContext) {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext, filterContext);

    return Pair.of(filterPlanNode, filterPlanNode.run());
  }

  private Pair<TransformOperator,
      AggregationGroupByOperator> buildOperators(BaseFilterOperator filterOperator,
      FilterPlanNode filterPlanNode, boolean isSwimlane,
      @Nullable ExpressionContext associatedExpContext) {
    assert _queryContext.getAggregationFunctions() != null;
    assert _queryContext.getGroupByExpressions() != null;

    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);

    // Use star-tree to solve the query if possible
    List<StarTreeV2> starTrees = _indexSegment.getStarTrees();
    if (starTrees != null && !StarTreeUtils.isStarTreeDisabled(_queryContext)) {
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
          StarTreeUtils.extractAggregationFunctionPairs(aggregationFunctions);
      if (aggregationFunctionColumnPairs != null) {
        Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
            StarTreeUtils.extractPredicateEvaluatorsMap(_indexSegment, _queryContext.getFilter(),
                filterPlanNode.getPredicateEvaluatorMap());
        if (predicateEvaluatorsMap != null) {
          for (StarTreeV2 starTreeV2 : starTrees) {
            if (StarTreeUtils.isFitForStarTree(starTreeV2.getMetadata(), aggregationFunctionColumnPairs,
                groupByExpressions, predicateEvaluatorsMap.keySet())) {
              TransformOperator transformOperator =
                  new StarTreeTransformPlanNode(starTreeV2, aggregationFunctionColumnPairs, groupByExpressions,
                      predicateEvaluatorsMap, null, _queryContext.getDebugOptions()).run();
              return Pair.of(transformOperator,
                  new AggregationGroupByOperator(_queryContext, groupByExpressions, transformOperator, numTotalDocs,
                  true));
            }
          }
        }
      }
    }

    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(aggregationFunctions, groupByExpressions);
    TransformOperator transformOperator =
        new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator, isSwimlane, associatedExpContext).run();

    return Pair.of(transformOperator,
        new AggregationGroupByOperator(_queryContext, groupByExpressions, transformOperator, numTotalDocs, false));
  }
}
