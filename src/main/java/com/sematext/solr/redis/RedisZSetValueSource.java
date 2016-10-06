package com.sematext.solr.redis;

import com.sematext.solr.redis.command.ZRange;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import java.io.IOException;
import java.util.Map;


/**
 * RedisZSetValueSource maps terms to their corresponding weights in a zset.
 *
 * @author smock
 */
public class RedisZSetValueSource extends ValueSource {
  /**
   * ValueSource to get strings from a field
   */
  private final IntFieldSource source;
  /**
   * Redis Key for the ZSet
   */
  private final String redisKey;
  /**
   * Redis command handler, instatiated from config
   */
  private final CommandHandler commandHandler;

  final Map<String, Float> results;

  /**
   * Constructor.
   * @param source Source for field to map to a zset
   * @param redisKey redis key for zset
   * @param commandHandler interface to redis
   */
  public RedisZSetValueSource(final IntFieldSource source, final String redisKey,
    final CommandHandler commandHandler) {
    this.source = source;
    this.redisKey = redisKey;
    this.commandHandler = commandHandler;

    final NamedList<String> paramList = new NamedList();
    paramList.add("key", redisKey);
    this.results = commandHandler.executeCommand(new ZRange(), SolrParams.toSolrParams(paramList));
  }

  /**
   * Constructor.
   * @return description of value source
   */
  @Override public String description() {
    return "Map field to a Redis ZSet weights";
  }

  /**
   * Constructor.
   * @return unique identifier for value source
   */
  @Override public int hashCode() {
    return source.hashCode() * redisKey.hashCode();
  }

  /**
   * Constructor.
   * @param o Object to compare to
   * @return Boolean
   */
  @Override public boolean equals(final Object o) {
    if (!(o instanceof RedisZSetValueSource)) {
      return false;
    }
    return this.source.equals(((RedisZSetValueSource)o).source) && this.redisKey.equals(((RedisZSetValueSource)o).redisKey);
  }

  /**
   * Constructor.
   * @param context context for index
   * @param readerContext reader context for index
   * @return function values
   * @throws IOException exception
   */
  @Override public FunctionValues getValues(final Map context,
    final LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals = source.getValues(context, readerContext);

    return new FunctionValues() {
      @Override
      public float floatVal(final int doc) {
        final String id = vals.strVal(doc);
        final Object v = results.get(id);
        if (v != null) {
          try {
            return Float.parseFloat(v.toString());
          } catch (final NumberFormatException e) {
            return 0;
          }
        } return 0;
      }

      @Override
      public int intVal(final int doc) {
        final String id = vals.toString(doc);
        final Object v = results.get(id);
        if (v != null) {
          try {
            return Integer.parseInt(v.toString());
          } catch (final NumberFormatException e) {
            return 0;
          }
        } return 0;
      }

      @Override
      public double doubleVal(final int doc) {
        return new Double(floatVal(doc));
      }

      @Override
      public String strVal(final int doc) {
        final String id = vals.toString(doc);
        final Object v = results.get(id);
        return v != null ? v.toString() : null;
      }

      @Override
      public String toString(final int doc) {
        return strVal(doc);
      }
    };
  }
}
