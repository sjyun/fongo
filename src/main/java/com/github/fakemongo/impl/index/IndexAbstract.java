package com.github.fakemongo.impl.index;

import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Filter;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.MongoException;
import org.bson.types.Binary;

import java.util.*;

/**
 * An index for the MongoDB.
 * <p/>
 * NOT Thread Safe. The ThreadSafety must be done by the caller.
 */
public abstract class IndexAbstract<T extends DBObject> {
  private final String name;
  private final DBObject keys;
  private final Set<String> fields;
  private final boolean unique;
  final String geoIndex;
  final ExpressionParser expressionParser = new ExpressionParser();
  // Contains all dbObject than field value can have
  final Map<T, List<T>> mapValues;
  int lookupCount = 0;

  IndexAbstract(String name, DBObject keys, boolean unique, Map<T, List<T>> mapValues, String geoIndex) throws MongoException {
    this.name = name;
    this.fields = Collections.unmodifiableSet(keys.keySet()); // Setup BEFORE keys.
    this.keys = prepareKeys(keys);
    this.unique = unique;
    this.mapValues = mapValues;
    this.geoIndex = geoIndex;

    for (Object value : keys.toMap().values()) {
      if (!(value instanceof String) && !(value instanceof Number)) {
        //com.mongodb.WriteConcernException: { "serverUsed" : "/127.0.0.1:27017" , "err" : "bad index key pattern { a: { n: 1 } }" , "code" : 10098 , "n" : 0 , "connectionId" : 543 , "ok" : 1.0}
        throw new MongoException(67, "bad index key pattern : " + keys);
      }
    }
  }

  private DBObject prepareKeys(DBObject keys) {
    DBObject nKeys = Util.clone(keys);
    if (!nKeys.containsField(FongoDBCollection.ID_KEY)) {
      // Remove _id for projection.
      nKeys.put("_id", 0);
    }
    // Transform 2d indexes into "1" (for now, can change later).
    for (Map.Entry<String, Object> entry : Util.entrySet(keys)) { // Work on keys to avoid ConcurrentModificationException
      if (entry.getValue().equals("2d") || entry.getValue().equals("2dsphere")) {
        nKeys.put(entry.getKey(), 1);
      }
      if (entry.getValue() instanceof Number && ((Number) entry.getValue()).longValue() < 0) {
        nKeys.put(entry.getKey(), 1); // Cannot mix -1 / +1 in projection.
      }
    }
    return nKeys;
  }

  static boolean isAsc(DBObject keys) {
    Object value = keys.toMap().values().iterator().next();
    if (value instanceof Number) {
      return ((Number) value).intValue() >= 1;
    }
    return false;
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  public boolean isGeoIndex() {
    return geoIndex != null;
  }

  public DBObject getKeys() {
    return keys;
  }

  public Set<String> getFields() {
    return fields;
  }

  /**
   * @param object    new object to insert in the index.
   * @param oldObject in update, old objet to remove from index.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> addOrUpdate(DBObject object, DBObject oldObject) {
    if (oldObject != null) {
      this.remove(oldObject); // TODO : optim ?
    }

    T key = getKeyFor(object);

    if (unique) {
      // Unique must check if he's really unique.
      if (mapValues.containsKey(key)) {
        return extractFields(object, key.keySet());
      }
      mapValues.put(key, Collections.singletonList(embedded(object))); // DO NOT CLONE !
    } else {
      // Extract previous values
      List<T> values = mapValues.get(key);
      if (values == null) {
        // Create if absent.
        values = new ArrayList<T>();
        mapValues.put(key, values);
      }

      // Add to values.
      T toAdd = embedded(object); // DO NOT CLONE ! Indexes must share the same object.
      values.add(toAdd);
    }
    return Collections.emptyList();
  }

  public abstract T embedded(DBObject object);

  /**
   * Check, in case of unique index, if we can add it.
   *
   * @param object
   * @param oldObject old object if update, null elsewhere.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> checkAddOrUpdate(DBObject object, DBObject oldObject) {
    if (unique) {
      DBObject key = getKeyFor(object);
      List<T> objects = mapValues.get(key);
      if (objects != null && !objects.contains(oldObject)) {
        List<List<Object>> fieldsForIndex = extractFields(object, getFields());
        return fieldsForIndex;
      }
    }
    return Collections.emptyList();
  }

  /**
   * Remove an object from the index.
   *
   * @param object to remove from the index.
   */
  public void remove(DBObject object) {
    DBObject key = getKeyFor(object);
    // Extract previous values
    List<T> values = mapValues.get(key);
    if (values != null) {
      // Last entry ? or uniqueness ?
      if (values.size() == 1) {
        mapValues.remove(key);
      } else {
        values.remove(object);
      }
    }
  }

  /**
   * Multiple add of objects.
   *
   * @param objects to add.
   * @return keys in error if uniqueness is not respected, empty collection otherwise.
   */
  public List<List<Object>> addAll(Iterable<DBObject> objects) {
    for (DBObject object : objects) {
      if (canHandle(object)) {
        List<List<Object>> nonUnique = addOrUpdate(object, null);
        // TODO(twillouer) : must handle writeConcern.
        if (!nonUnique.isEmpty()) {
          return nonUnique;
        }
      }
    }
    return Collections.emptyList();
  }

  // Only for unique index and for query with values. ($in doens't work by example.)
  public List<T> get(DBObject query) {
    if (!unique) {
      throw new IllegalStateException("get is only for unique index");
    }
    lookupCount++;

    DBObject key = getKeyFor(query);
    return mapValues.get(key);
  }

  // @Nonnull
  public Collection<T> retrieveObjects(DBObject query) {
    // Optimization
    if (unique && query.keySet().size() == 1) {
      Object key = query.toMap().values().iterator().next();
      if (!(key instanceof DBObject || key instanceof Binary || key instanceof byte[])) {
        List<T> result = get(query);
        if (result != null) {
          return result;
        }
      }
    }

    lookupCount++;

    // Filter for the key.
    Filter filterKey = expressionParser.buildFilter(query, getFields());
    // Filter for the data.
    Filter filter = expressionParser.buildFilter(query);
    List<T> result = new ArrayList<T>();
    for (Map.Entry<T, List<T>> entry : mapValues.entrySet()) {
      if (filterKey.apply(entry.getKey())) {
        for (T object : entry.getValue()) {
          if (filter.apply(object)) {
            result.add(object); // DO NOT CLONE ! need for update.
          }
        }
      }
    }
    return result;
  }

  public long getLookupCount() {
    return lookupCount;
  }

  public int size() {
    int size = 0;
    if (unique) {
      size = mapValues.size();
    } else {
      for (Map.Entry<T, List<T>> entry : mapValues.entrySet()) {
        size += entry.getValue().size();
      }
    }
    return size;
  }

  public List<DBObject> values() {
    List<DBObject> values = new ArrayList<DBObject>(mapValues.size() * 10);
    for (List<T> objects : mapValues.values()) {
      values.addAll(objects);
    }
    return values;
  }

  public void clear() {
    mapValues.clear();
  }

  /**
   * Return true if index can handle this query.
   *
   * @param queryFields fields of the query.
   * @return true if index can be used.
   */
  public boolean canHandle(final DBObject queryFields) {
    if (queryFields == null) {
      return false;
    }

    //get keys including embedded indexes
    for (String field : fields) {
      if (!queryFields.containsField(field) && !keyEmbeddedFieldMatch(field, queryFields)) {
        return false;
      }
    }
    return true;
  }

  private boolean keyEmbeddedFieldMatch(String field, DBObject queryFields) {
    //if field embedded field type
    String[] fieldParts = field.split("\\.");
    if (fieldParts.length == 0) {
      return false;
    }

    DBObject searchQueryFields = queryFields;
    int count = 0;
    for (String fieldPart : fieldParts) {
      count++;
      if (searchQueryFields instanceof BasicDBList) {
        // when it's a list, there's no need to investigate nested documents
        return true;
      } else if (!searchQueryFields.containsField(fieldPart)) {
        return false;
      } else if (searchQueryFields.get(fieldPart) instanceof DBObject) {
        searchQueryFields = (DBObject) searchQueryFields.get(fieldPart);
      }
    }

    return fieldParts.length == count;
  }

  @Override
  public String toString() {
    return "Index{" +
        "name='" + name + '\'' +
        '}';
  }

  /**
   * Create the key for the hashmap.
   *
   * @param object
   * @return
   */
  T getKeyFor(DBObject object) {
    DBObject applyProjections = FongoDBCollection.applyProjections(object, keys);
    return (T) applyProjections;
  }

  private List<List<Object>> extractFields(DBObject dbObject, Collection<String> fields) {
    List<List<Object>> fieldValue = new ArrayList<List<Object>>();
    for (String field : fields) {
      List<Object> embeddedValues = expressionParser.getEmbeddedValues(field, dbObject);
      fieldValue.add(embeddedValues);
    }
    return fieldValue;
  }
}
