package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.FilteredAggregationExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;

public class FilteredAggregationFunction implements AggregationFunction {
  private FilteredAggregationExpressionContext _filteredAggregationExpressionContext;

  public FilteredAggregationFunction(FilteredAggregationExpressionContext filteredAggregationExpressionContext) {
    _filteredAggregationExpressionContext = filteredAggregationExpressionContext;
  }

  @Override
  public AggregationFunctionType getType() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getType();
  }

  @Override
  public String getColumnName() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getColumnName();
  }

  @Override
  public String getResultColumnName() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getResultColumnName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getInputExpressions();
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _filteredAggregationExpressionContext.getAggregationFunction().createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _filteredAggregationExpressionContext.getAggregationFunction().createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _filteredAggregationExpressionContext.getAggregationFunction().extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _filteredAggregationExpressionContext.getAggregationFunction().extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    return _filteredAggregationExpressionContext.getAggregationFunction().merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _filteredAggregationExpressionContext.getAggregationFunction().isIntermediateResultComparable();
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getIntermediateResultColumnType();
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return _filteredAggregationExpressionContext.getAggregationFunction().getFinalResultColumnType();
  }

  @Override
  public Comparable extractFinalResult(Object o) {
    return _filteredAggregationExpressionContext.getAggregationFunction().extractFinalResult(o);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder, Map map) {

  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder, Map map) {

  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, Map map) {
    //TransformFunction transformFunction = TransformFunctionFactory.get(_filteredAggregationExpressionContext.getFilterContex);
    //_filteredAggregationExpressionContext.
  }
}
