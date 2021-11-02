package org.apache.pinot.core.operator.filter.predicate;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.apache.pinot.common.request.context.predicate.Predicate.Type.REGEXP_LIKE;


public class NativeFSTBasedRegexpPredicateEvaluatorFactory {
  /**
   * Creates a predicate evaluator which matches the regexp query pattern using
   * FST Index available. FST Index is not yet present for consuming segments,
   * so use newAutomatonBasedEvaluator for consuming segments.
   *
   * @param fstIndexReader FST Index reader
   * @param dictionary Dictionary for the column
   * @param regexpQuery input query to match
   * @return Predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newFSTBasedEvaluator(TextIndexReader fstIndexReader,
      Dictionary dictionary, String regexpQuery) {
    return new NativeFSTBasedRegexpPredicateEvaluatorFactory.NativeFSTBasedRegexpPredicateEvaluator(fstIndexReader, dictionary,
        regexpQuery);
  }

  /**
   * Creates a predicate evaluator which uses regex matching logic which is similar to
   * FSTBasedRegexpPredicateEvaluator. This predicate evaluator is used for consuming
   * segments and is there to make sure results are consistent between consuming and
   * rolled out segments when FST index is enabled.
   *
   * @param dictionary Dictionary for the column
   * @param regexpQuery input query to match
   * @return Predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newAutomatonBasedEvaluator(Dictionary dictionary,
      String regexpQuery) {
    return new FSTBasedRegexpPredicateEvaluatorFactory.AutomatonBasedRegexpPredicateEvaluator(regexpQuery, dictionary);
  }

  /**
   * Matches regexp query using FSTIndexReader.
   */
  private static class NativeFSTBasedRegexpPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    private final TextIndexReader _fstIndexReader;
    private final String _searchQuery;
    private final ImmutableRoaringBitmap _dictIds;
    private final Dictionary _dictionary;

    public NativeFSTBasedRegexpPredicateEvaluator(TextIndexReader fstIndexReader, Dictionary dictionary, String searchQuery) {
      _dictionary = dictionary;
      _fstIndexReader = fstIndexReader;
      _searchQuery = searchQuery;
      _dictIds = _fstIndexReader.getDictIds(_searchQuery);
    }

    @Override
    public boolean isAlwaysFalse() {
      return _dictIds.isEmpty();
    }

    @Override
    public boolean isAlwaysTrue() {
      return _dictIds.getCardinality() == _dictionary.length();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return REGEXP_LIKE;
    }

    @Override
    public boolean applySV(int dictId) {
      return _dictIds.contains(dictId);
    }

    @Override
    public int[] getMatchingDictIds() {
      return _dictIds.toArray();
    }
  }
}
