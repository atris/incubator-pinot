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
package org.apache.pinot.core.query.request.context;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.util.MemoizedClassAssociation;


/**
 * The {@code QueryContext} class encapsulates all the query related information extracted from the wiring Object.
 * <p>The query engine should work on the QueryContext instead of the wiring Object for the following benefits:
 * <ul>
 *   <li>
 *     Execution layer can be decoupled from the wiring layer, so that changes for one layer won't affect the other
 *     layer.
 *   </li>
 *   <li>
 *     It is very hard to change wiring Object because it involves protocol change, so we should make it as generic as
 *     possible to support future features. Instead, QueryContext is extracted from the wiring Object within each
 *     Broker/Server, and changing it won't cause any protocol change, so we can upgrade it along with the new feature
 *     support in query engine as needed. Also, because of this, we don't have to make QueryContext very generic, which
 *     can help save the overhead of handling generic Objects (e.g. we can pre-compute the Predicates instead of the
 *     using the generic Expressions).
 *   </li>
 *   <li>
 *     In case we need to change the wiring Object (e.g. switch from Thrift to Protobuf), we don't need to change the
 *     whole query engine.
 *   </li>
 *   <li>
 *     We can also add some helper variables or methods in the context classes which can be shared for all segments to
 *     reduce the repetitive work for each segment.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueryContext {
  private final String _tableName;
  private final List<ExpressionContext> _selectExpressions;
  private final List<String> _aliasList;
  private final FilterContext _filter;
  private final List<ExpressionContext> _groupByExpressions;
  private final FilterContext _havingFilter;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _limit;
  private final int _offset;
  private final Map<String, String> _queryOptions;
  private final Map<String, String> _debugOptions;

  // Keep the BrokerRequest to make incremental changes
  // TODO: Remove it once the whole query engine is using the QueryContext
  private final BrokerRequest _brokerRequest;

  private final Function<Class<?>, Map<?, ?>> _sharedValues = MemoizedClassAssociation.of(ConcurrentHashMap::new);

  // Pre-calculate the aggregation functions and columns for the query so that it can be shared across all the segments
  private AggregationFunction[] _aggregationFunctions;

  private List<Pair<AggregationFunction, FilterContext>> _filteredAggregations;

  private boolean _hasFilteredAggregations;
  private Map<Pair<FunctionContext, FilterContext>, Integer> _filteredAggregationsIndexMap;
  private Map<FunctionContext, Integer> _aggregationFunctionIndexMap;
  private Set<String> _columns;

  // Other properties to be shared across all the segments
  // End time in milliseconds for the query
  private long _endTimeMs;
  // Whether to enable prefetch for the query
  private boolean _enablePrefetch;
  // Maximum number of threads used to execute the query
  private int _maxExecutionThreads = InstancePlanMakerImplV2.DEFAULT_MAX_EXECUTION_THREADS;
  // The following properties apply to group-by queries
  // Maximum initial capacity of the group-by result holder
  private int _maxInitialResultHolderCapacity = InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  // Limit of number of groups stored in each segment
  private int _numGroupsLimit = InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT;
  // Minimum number of groups to keep per segment when trimming groups for SQL GROUP BY
  private int _minSegmentGroupTrimSize = InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE;
  // Minimum number of groups to keep across segments when trimming groups for SQL GROUP BY
  private int _minServerGroupTrimSize = InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE;
  // Trim threshold to use for server combine for SQL GROUP BY
  private int _groupTrimThreshold = InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD;

  private QueryContext(String tableName, List<ExpressionContext> selectExpressions, List<String> aliasList,
      @Nullable FilterContext filter, @Nullable List<ExpressionContext> groupByExpressions,
      @Nullable FilterContext havingFilter, @Nullable List<OrderByExpressionContext> orderByExpressions, int limit,
      int offset, Map<String, String> queryOptions, @Nullable Map<String, String> debugOptions,
      BrokerRequest brokerRequest) {
    _tableName = tableName;
    _selectExpressions = selectExpressions;
    _aliasList = Collections.unmodifiableList(aliasList);
    _filter = filter;
    _groupByExpressions = groupByExpressions;
    _havingFilter = havingFilter;
    _orderByExpressions = orderByExpressions;
    _limit = limit;
    _offset = offset;
    _queryOptions = queryOptions;
    _debugOptions = debugOptions;
    _brokerRequest = brokerRequest;
  }

  /**
   * Returns the table name.
   */
  public String getTableName() {
    return _tableName;
  }

  /**
   * Returns a list of expressions in the SELECT clause.
   */
  public List<ExpressionContext> getSelectExpressions() {
    return _selectExpressions;
  }

  /**
   * Returns an unmodifiable list from the expression to its alias.
   */
  public List<String> getAliasList() {
    return _aliasList;
  }

  /**
   * Returns the filter in the WHERE clause, or {@code null} if there is no WHERE clause.
   */
  @Nullable
  public FilterContext getFilter() {
    return _filter;
  }

  /**
   * Returns a list of expressions in the GROUP-BY clause, or {@code null} if there is no GROUP-BY clause.
   */
  @Nullable
  public List<ExpressionContext> getGroupByExpressions() {
    return _groupByExpressions;
  }

  /**
   * Returns the filter in the HAVING clause, or {@code null} if there is no HAVING clause.
   */
  @Nullable
  public FilterContext getHavingFilter() {
    return _havingFilter;
  }

  /**
   * Returns a list of order-by expressions in the ORDER-BY clause, or {@code null} if there is no ORDER-BY clause.
   */
  @Nullable
  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  /**
   * Returns the limit of the query.
   */
  public int getLimit() {
    return _limit;
  }

  /**
   * Returns the offset of the query.
   */
  public int getOffset() {
    return _offset;
  }

  /**
   * Returns the query options of the query.
   */
  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  /**
   * Returns the debug options of the query, or {@code null} if not exist.
   */
  @Nullable
  public Map<String, String> getDebugOptions() {
    return _debugOptions;
  }

  /**
   * Returns the BrokerRequest where the QueryContext is extracted from.
   */
  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  /**
   * Returns the aggregation functions for the query, or {@code null} if the query does not have any aggregation.
   */
  @Nullable
  public AggregationFunction[] getAggregationFunctions() {
    return _aggregationFunctions;
  }

  /**
   * Returns the filtered aggregations for a query
   */
  @Nullable
  public List<Pair<AggregationFunction, FilterContext>> getFilteredAggregations() {
    return _filteredAggregations;
  }

  /**
   * Returns the filtered aggregation expressions for the query.
   */
  public boolean isHasFilteredAggregations() {
    return _hasFilteredAggregations;
  }

  /**
   * Returns a map from the AGGREGATION FunctionContext to the index of the corresponding AggregationFunction in the
   * aggregation functions array.
   */
  @Nullable
  public Map<FunctionContext, Integer> getAggregationFunctionIndexMap() {
    return _aggregationFunctionIndexMap;
  }

  /**
   * Returns a map from the expression of a filtered aggregation to the index of corresponding AggregationFunction
   * in the aggregation functions array
   * @return
   */
  @Nullable
  public Map<Pair<FunctionContext, FilterContext>, Integer> getFilteredAggregationsIndexMap() {
    return _filteredAggregationsIndexMap;
  }

  /**
   * Returns the columns (IDENTIFIER expressions) in the query.
   */
  public Set<String> getColumns() {
    return _columns;
  }

  public long getEndTimeMs() {
    return _endTimeMs;
  }

  public void setEndTimeMs(long endTimeMs) {
    _endTimeMs = endTimeMs;
  }

  public boolean isEnablePrefetch() {
    return _enablePrefetch;
  }

  public void setEnablePrefetch(boolean enablePrefetch) {
    _enablePrefetch = enablePrefetch;
  }

  public int getMaxExecutionThreads() {
    return _maxExecutionThreads;
  }

  public void setMaxExecutionThreads(int maxExecutionThreads) {
    _maxExecutionThreads = maxExecutionThreads;
  }

  public int getMaxInitialResultHolderCapacity() {
    return _maxInitialResultHolderCapacity;
  }

  public void setMaxInitialResultHolderCapacity(int maxInitialResultHolderCapacity) {
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
  }

  public int getNumGroupsLimit() {
    return _numGroupsLimit;
  }

  public void setNumGroupsLimit(int numGroupsLimit) {
    _numGroupsLimit = numGroupsLimit;
  }

  public int getMinSegmentGroupTrimSize() {
    return _minSegmentGroupTrimSize;
  }

  public void setMinSegmentGroupTrimSize(int minSegmentGroupTrimSize) {
    _minSegmentGroupTrimSize = minSegmentGroupTrimSize;
  }

  public int getMinServerGroupTrimSize() {
    return _minServerGroupTrimSize;
  }

  public void setMinServerGroupTrimSize(int minServerGroupTrimSize) {
    _minServerGroupTrimSize = minServerGroupTrimSize;
  }

  public int getGroupTrimThreshold() {
    return _groupTrimThreshold;
  }

  public void setGroupTrimThreshold(int groupTrimThreshold) {
    _groupTrimThreshold = groupTrimThreshold;
  }

  /**
   * Gets or computes a value of type {@code V} associated with a key of type {@code K} so that it can be shared
   * within the scope of a query.
   * @param type the type of the value produced, guarantees type pollution is impossible.
   * @param key the key used to determine if the value has already been computed.
   * @param mapper A function to apply the first time a key is encountered to construct the value.
   * @param <K> the key type
   * @param <V> the value type
   * @return the shared value
   */
  public <K, V> V getOrComputeSharedValue(Class<V> type, K key, Function<K, V> mapper) {
    return ((ConcurrentHashMap<K, V>) _sharedValues.apply(type)).computeIfAbsent(key, mapper);
  }

  /**
   * NOTE: For debugging only.
   */
  @Override
  public String toString() {
    return "QueryContext{" + "_tableName='" + _tableName + '\'' + ", _selectExpressions=" + _selectExpressions
        + ", _aliasList=" + _aliasList + ", _filter=" + _filter + ", _groupByExpressions=" + _groupByExpressions
        + ", _havingFilter=" + _havingFilter + ", _orderByExpressions=" + _orderByExpressions + ", _limit=" + _limit
        + ", _offset=" + _offset + ", _queryOptions=" + _queryOptions + ", _debugOptions=" + _debugOptions
        + ", _brokerRequest=" + _brokerRequest + '}';
  }

  public static class Builder {
    private String _tableName;
    private List<ExpressionContext> _selectExpressions;
    private List<String> _aliasList;
    private FilterContext _filter;
    private List<ExpressionContext> _groupByExpressions;
    private FilterContext _havingFilter;
    private List<OrderByExpressionContext> _orderByExpressions;
    private int _limit;
    private int _offset;
    private Map<String, String> _queryOptions;
    private Map<String, String> _debugOptions;
    private BrokerRequest _brokerRequest;

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setSelectExpressions(List<ExpressionContext> selectExpressions) {
      _selectExpressions = selectExpressions;
      return this;
    }

    public Builder setAliasList(List<String> aliasList) {
      _aliasList = aliasList;
      return this;
    }

    public Builder setFilter(@Nullable FilterContext filter) {
      _filter = filter;
      return this;
    }

    public Builder setGroupByExpressions(@Nullable List<ExpressionContext> groupByExpressions) {
      _groupByExpressions = groupByExpressions;
      return this;
    }

    public Builder setHavingFilter(@Nullable FilterContext havingFilter) {
      _havingFilter = havingFilter;
      return this;
    }

    public Builder setOrderByExpressions(@Nullable List<OrderByExpressionContext> orderByExpressions) {
      _orderByExpressions = orderByExpressions;
      return this;
    }

    public Builder setLimit(int limit) {
      _limit = limit;
      return this;
    }

    public Builder setOffset(int offset) {
      _offset = offset;
      return this;
    }

    public Builder setQueryOptions(@Nullable Map<String, String> queryOptions) {
      _queryOptions = queryOptions;
      return this;
    }

    public Builder setDebugOptions(@Nullable Map<String, String> debugOptions) {
      _debugOptions = debugOptions;
      return this;
    }

    public Builder setBrokerRequest(BrokerRequest brokerRequest) {
      _brokerRequest = brokerRequest;
      return this;
    }

    public QueryContext build() {
      // TODO: Add validation logic here

      if (_queryOptions == null) {
        _queryOptions = Collections.emptyMap();
      }
      QueryContext queryContext =
          new QueryContext(_tableName, _selectExpressions, _aliasList, _filter, _groupByExpressions, _havingFilter,
              _orderByExpressions, _limit, _offset, _queryOptions, _debugOptions, _brokerRequest);

      // Pre-calculate the aggregation functions and columns for the query
      generateAggregationFunctions(queryContext);
      extractColumns(queryContext);

      return queryContext;
    }

    /**
     * Helper method to generate the aggregation functions for the query.
     */
    private void generateAggregationFunctions(QueryContext queryContext) {
      List<AggregationFunction> aggregationFunctions = new ArrayList<>();
      List<Pair<AggregationFunction, FilterContext>> filteredAggregations = new ArrayList<>();
      Map<FunctionContext, Integer> aggregationFunctionIndexMap = new HashMap<>();
      Map<Pair<FunctionContext, FilterContext>, Integer> filterExpressionIndexMap = new HashMap<>();

      // Add aggregation functions in the SELECT clause
      // NOTE: DO NOT deduplicate the aggregation functions in the SELECT clause because that involves protocol change.
      List<Pair<FilterContext, FunctionContext>> aggregationsInSelect = new ArrayList<>();
      for (ExpressionContext selectExpression : queryContext._selectExpressions) {
        getAggregations(selectExpression, aggregationsInSelect);
      }
      for (Pair<FilterContext, FunctionContext> pair : aggregationsInSelect) {
        FunctionContext function = pair.getRight();
        int functionIndex = filteredAggregations.size();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(function, queryContext);

        FilterContext filterContext = null;
        // If the left pair is not null, implies a filtered aggregation
        if (pair.getLeft() != null) {
          if (_groupByExpressions != null) {
            throw new IllegalStateException("GROUP BY with FILTER clauses is not supported");
          }
          queryContext._hasFilteredAggregations = true;
          filterContext = pair.getLeft();
          Pair<FunctionContext, FilterContext> filterContextPair =
              Pair.of(function, filterContext);
          if (!filterExpressionIndexMap.containsKey(filterContextPair)) {
            int filterMapIndex = filterExpressionIndexMap.size();
            filterExpressionIndexMap.put(filterContextPair, filterMapIndex);
          }
        }
        filteredAggregations.add(Pair.of(aggregationFunction, filterContext));
        aggregationFunctionIndexMap.put(function, functionIndex);
      }

      // Add aggregation functions in the HAVING clause but not in the SELECT clause
      if (queryContext._havingFilter != null) {
        List<Pair<FilterContext, FunctionContext>> aggregationsInHaving = new ArrayList<>();
        getAggregations(queryContext._havingFilter, aggregationsInHaving);
        for (Pair<FilterContext, FunctionContext> pair : aggregationsInHaving) {
          FunctionContext function = pair.getRight();
          if (!aggregationFunctionIndexMap.containsKey(function)) {
            int functionIndex = filteredAggregations.size();
            filteredAggregations.add(Pair.of(
                AggregationFunctionFactory.getAggregationFunction(function, queryContext), null));
            aggregationFunctionIndexMap.put(function, functionIndex);
          }
        }
      }

      // Add aggregation functions in the ORDER-BY clause but not in the SELECT or HAVING clause
      if (queryContext._orderByExpressions != null) {
        List<Pair<FilterContext, FunctionContext>> aggregationsInOrderBy = new ArrayList<>();
        for (OrderByExpressionContext orderByExpression : queryContext._orderByExpressions) {
          getAggregations(orderByExpression.getExpression(), aggregationsInOrderBy);
        }
        for (Pair<FilterContext, FunctionContext> pair : aggregationsInOrderBy) {
          FunctionContext function = pair.getRight();
          if (!aggregationFunctionIndexMap.containsKey(function)) {
            int functionIndex = filteredAggregations.size();
            filteredAggregations.add(Pair.of(
                AggregationFunctionFactory.getAggregationFunction(function, queryContext), null));
            aggregationFunctionIndexMap.put(function, functionIndex);
          }
        }
      }

      if (!filteredAggregations.isEmpty()) {
        for (Pair<AggregationFunction, FilterContext> pair : filteredAggregations) {
          aggregationFunctions.add(pair.getLeft());
        }

        queryContext._aggregationFunctions = aggregationFunctions.toArray(new AggregationFunction[0]);
        queryContext._filteredAggregations = filteredAggregations;
        queryContext._aggregationFunctionIndexMap = aggregationFunctionIndexMap;
        queryContext._filteredAggregationsIndexMap = filterExpressionIndexMap;
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts from the given expression.
     *
     * NOTE: The left pair of aggregations should be set only for filtered aggregations
     */
    private static void getAggregations(ExpressionContext expression,
        List<Pair<FilterContext, FunctionContext>> aggregations) {
      FunctionContext function = expression.getFunction();
      if (function == null) {
        return;
      }
      if (function.getType() == FunctionContext.Type.AGGREGATION) {
        // Aggregation
        aggregations.add(Pair.of(null, function));
      } else {
        List<ExpressionContext> arguments = function.getArguments();
        if (function.getFunctionName().equalsIgnoreCase("filter")) {
          // Filtered aggregation
          Preconditions.checkState(arguments.size() == 2, "FILTER must contain 2 arguments");
          FunctionContext aggregation = arguments.get(0).getFunction();
          Preconditions.checkState(aggregation != null && aggregation.getType() == FunctionContext.Type.AGGREGATION,
              "First argument of FILTER must be an aggregation function");
          ExpressionContext filterExpression = arguments.get(1);
          Preconditions.checkState(filterExpression.getFunction() != null
                  && filterExpression.getFunction().getType() == FunctionContext.Type.TRANSFORM,
              "Second argument of FILTER must be a filter expression");
          FilterContext filter = RequestContextUtils.getFilter(filterExpression);

          aggregations.add(Pair.of(filter, aggregation));
        } else {
          // Transform
          for (ExpressionContext argument : arguments) {
            getAggregations(argument, aggregations);
          }
        }
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts from the given filter.
     */
    private static void getAggregations(FilterContext filter,
        List<Pair<FilterContext, FunctionContext>> aggregations) {
      List<FilterContext> children = filter.getChildren();
      if (children != null) {
        for (FilterContext child : children) {
          getAggregations(child, aggregations);
        }
      } else {
        getAggregations(filter.getPredicate().getLhs(), aggregations);
      }
    }

    /**
     * Helper method to extract the columns (IDENTIFIER expressions) for the query.
     */
    private void extractColumns(QueryContext query) {
      Set<String> columns = new HashSet<>();

      for (ExpressionContext expression : query._selectExpressions) {
        expression.getColumns(columns);
      }
      if (query._filter != null) {
        query._filter.getColumns(columns);
      }
      if (query._groupByExpressions != null) {
        for (ExpressionContext expression : query._groupByExpressions) {
          expression.getColumns(columns);
        }
      }
      if (query._havingFilter != null) {
        query._havingFilter.getColumns(columns);
      }
      if (query._orderByExpressions != null) {
        for (OrderByExpressionContext orderByExpression : query._orderByExpressions) {
          orderByExpression.getColumns(columns);
        }
      }

      // NOTE: Also gather columns from the input expressions of the aggregation functions because for certain types of
      //       aggregation (e.g. DistinctCountThetaSketch), some input expressions are compiled while constructing the
      //       aggregation function.
      if (query._aggregationFunctions != null) {
        for (AggregationFunction aggregationFunction : query._aggregationFunctions) {
          List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
          for (ExpressionContext expression : inputExpressions) {
            expression.getColumns(columns);
          }
        }
      }

      query._columns = columns;
    }
  }
}
