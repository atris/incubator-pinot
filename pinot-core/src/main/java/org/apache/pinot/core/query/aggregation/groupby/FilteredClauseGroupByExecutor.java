package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.FilterableAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


public class FilteredClauseGroupByExecutor extends DefaultGroupByExecutor {
  /**
   * Constructor for the class.
   *  @param queryContext Query context
   * @param groupByExpressions Array of group-by expressions
   * @param transformOperator Transform operator
   */
  public FilteredClauseGroupByExecutor(QueryContext queryContext,
      ExpressionContext[] groupByExpressions,
      TransformOperator transformOperator) {
    super(queryContext, groupByExpressions, transformOperator);
  }

  @Override
  public void process(TransformBlock transformBlock) {
    if (!(transformBlock instanceof CombinedTransformBlock)) {
      throw new IllegalArgumentException("FilteredClauseAggregationExecutor only works"
          + "with CombinedTransformBlock");
    }

    CombinedTransformBlock combinedTransformBlock = (CombinedTransformBlock) transformBlock;
    Map<ExpressionContext, TransformBlock> transformBlockMap = combinedTransformBlock.getTransformBlockMap();
    int numAggregationFunctions = _aggregationFunctions.length;

    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];

      if (aggregationFunction instanceof FilterableAggregationFunction) {
        FilterableAggregationFunction filterableAggregationFunction =
            (FilterableAggregationFunction) aggregationFunction;

        TransformBlock innerTransformBlock = transformBlockMap
            .get(filterableAggregationFunction.getAssociatedExpressionContext());

        if (innerTransformBlock != null) {
          doProcess(innerTransformBlock, combinedTransformBlock.getNonFilteredAggBlock(), i);
        }
      } else {
        doProcess(combinedTransformBlock.getNonFilteredAggBlock(), combinedTransformBlock.getNonFilteredAggBlock(), i);
      }
    }
  }

  private void doProcess(TransformBlock transformBlock, TransformBlock fooBlock, int i) {

    if (transformBlock == null) {
      return;
    }

    // Generate group keys
    // NOTE: groupKeyGenerator will limit the number of groups. Once reaching limit, no new group will be generated
    if (_hasMVGroupByExpression) {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _mvGroupKeys);
    } else {
      //_groupKeyGenerator.generateKeysForBlock(transformBlock, _svGroupKeys);
      _groupKeyGenerator.generateKeysForBlock(fooBlock, _svGroupKeys);
    }

    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();
    int length = fooBlock.getNumDocs();

    GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
    groupByResultHolder.ensureCapacity(capacityNeeded);
    aggregate(transformBlock, length, i);
  }
}
