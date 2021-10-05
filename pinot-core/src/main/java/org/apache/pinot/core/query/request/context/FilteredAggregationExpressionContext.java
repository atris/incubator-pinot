package org.apache.pinot.core.query.request.context;

import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;

public class FilteredAggregationExpressionContext {
  private final AggregationFunction _aggregationFunction;
  private final FilterContext _filterContext;

  public FilteredAggregationExpressionContext(AggregationFunction aggregationFunction, FilterContext filterContext) {
    _aggregationFunction = aggregationFunction;
    _filterContext = filterContext;
  }

  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  public FilterContext getFilterContext() {
    return _filterContext;
  }

  @Override
  public String toString() {
    return _aggregationFunction.toString() + (" Filter " + _filterContext.toString());
  }
}
