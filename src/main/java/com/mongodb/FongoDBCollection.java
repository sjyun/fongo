package com.mongodb;

import static org.bson.util.Assertions.isTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.bson.BSON;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.objenesis.ObjenesisStd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fakemongo.FongoException;
import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Filter;
import com.github.fakemongo.impl.Tuple2;
import com.github.fakemongo.impl.UpdateEngine;
import com.github.fakemongo.impl.Util;
import com.github.fakemongo.impl.geo.GeoUtil;
import com.github.fakemongo.impl.geo.LatLong;
import com.github.fakemongo.impl.index.GeoIndex;
import com.github.fakemongo.impl.index.IndexAbstract;
import com.github.fakemongo.impl.index.IndexFactory;
import com.github.fakemongo.impl.text.TextSearch;

/**
 * fongo override of com.mongodb.DBCollection
 * you shouldn't need to use this class directly
 *
 * @author jon
 */
public class FongoDBCollection extends DBCollection {
  private final static Logger LOG = LoggerFactory.getLogger(FongoDBCollection.class);

  public static final String ID_KEY = "_id";

  private static final String ID_NAME_INDEX = "_id_";
  private final FongoDB fongoDb;
  private final ExpressionParser expressionParser;
  private final UpdateEngine updateEngine;
  private final boolean nonIdCollection;
  private final ExpressionParser.ObjectComparator objectComparator;
  // Fields/Index
  private final List<IndexAbstract> indexes = new ArrayList<IndexAbstract>();
  private final IndexAbstract _idIndex;

  public FongoDBCollection(FongoDB db, String name) {
    super(db, name);
    this.fongoDb = db;
    this.nonIdCollection = name.startsWith("system");
    this.expressionParser = new ExpressionParser();
    this.updateEngine = new UpdateEngine();
    this.objectComparator = expressionParser.buildObjectComparator(true);
    this._idIndex = IndexFactory.create(ID_KEY, new BasicDBObject(ID_KEY, 1), true);
    this.indexes.add(_idIndex);
    if (!this.nonIdCollection) {
      this.createIndex(new BasicDBObject(ID_KEY, 1), new BasicDBObject("name", ID_NAME_INDEX));
    }
  }

  private CommandResult insertResult(int updateCount) {
    CommandResult result = fongoDb.okResult();
    result.put("n", updateCount);
    return result;
  }

  private CommandResult updateResult(int updateCount, boolean updatedExisting) {
    CommandResult result = fongoDb.okResult();
    result.put("n", updateCount);
    result.put("updatedExisting", updatedExisting);
    return result;
  }

  @Override
  public synchronized WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) throws MongoException {
    return insert(Arrays.asList(arr), concern, encoder);
  }

  private DBObject encodeDecode(DBObject dbObject, DBEncoder encoder) {
    if (dbObject instanceof LazyDBObject) {
      if (encoder == null) {
        encoder = DefaultDBEncoder.FACTORY.create();
      }
      OutputBuffer outputBuffer = new BasicOutputBuffer();
      encoder.writeObject(outputBuffer, dbObject);
      return DefaultDBDecoder.FACTORY.create().decode(outputBuffer.toByteArray(), this);
    }
    return dbObject;
  }

  @Override
  public synchronized WriteResult insert(List<DBObject> toInsert, WriteConcern concern, DBEncoder encoder) {
    for (DBObject obj : toInsert) {
      DBObject cloned = filterLists(Util.cloneIdFirst(encodeDecode(obj, encoder)));
      if (LOG.isDebugEnabled()) {
        LOG.debug("insert: " + cloned);
      }
      ObjectId id = putIdIfNotPresent(cloned);
      // Save the id field in the caller.
      if (!(obj instanceof LazyDBObject) && obj.get(ID_KEY) == null) {
        obj.put(ID_KEY, Util.clone(id));
      }

      putSizeCheck(cloned, concern);
    }
    return new WriteResult(insertResult(toInsert.size()), concern);
  }

  boolean enforceDuplicates(WriteConcern concern) {
    WriteConcern writeConcern = concern == null ? getWriteConcern() : concern;
    return writeConcern._w instanceof Number && ((Number) writeConcern._w).intValue() > 0;
  }

  public ObjectId putIdIfNotPresent(DBObject obj) {
    Object object = obj.get(ID_KEY);
    if (object == null) {
      ObjectId id = new ObjectId(new Date()); // No more "notNew"
      id.notNew();
      obj.put(ID_KEY, Util.clone(id));
      return id;
    } else if (object instanceof ObjectId) {
      ObjectId id = (ObjectId) object;
      id.notNew();
      return id;
    }

    return null;
  }

  public void putSizeCheck(DBObject obj, WriteConcern concern) {
    if (_idIndex.size() > 100000) {
      throw new FongoException("Whoa, hold up there.  Fongo's designed for lightweight testing.  100,000 items per collection max");
    }

    addToIndexes(obj, null, concern);
  }

  public DBObject filterLists(DBObject dbo) {
    if (dbo == null) {
      return null;
    }
    dbo = Util.clone(dbo);
    for (Map.Entry<String, Object> entry : Util.entrySet(dbo)) {
      Object replacementValue = replaceListAndMap(entry.getValue());
      dbo.put(entry.getKey(), replacementValue);
    }
    return dbo;
  }

  public Object replaceListAndMap(Object value) {
    Object replacementValue = BSON.applyEncodingHooks(value);
    if (replacementValue instanceof DBObject) {
      replacementValue = filterLists((DBObject) replacementValue);
    } else if (replacementValue instanceof List) {
      BasicDBList list = new BasicDBList();
      for (Object listItem : (List) replacementValue) {
        list.add(replaceListAndMap(listItem));
      }
      replacementValue = list;
    } else if (replacementValue instanceof Object[]) {
      BasicDBList list = new BasicDBList();
      for (Object listItem : (Object[]) replacementValue) {
        list.add(replaceListAndMap(listItem));
      }
      replacementValue = list;
    } else if (replacementValue instanceof Map) {
      BasicDBObject newDbo = new BasicDBObject();
      //noinspection unchecked
      for (Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) ((Map) replacementValue).entrySet()) {
        newDbo.put(entry.getKey(), replaceListAndMap(entry.getValue()));
      }
      replacementValue = newDbo;
    } else if (replacementValue instanceof Binary) {
      replacementValue = ((Binary) replacementValue).getData();
    }
    return replacementValue;
  }


  protected void fInsert(DBObject obj, WriteConcern concern) {
    putIdIfNotPresent(obj);
    putSizeCheck(obj, concern);
  }


  @Override
  public synchronized WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern,
                                         DBEncoder encoder) throws MongoException {

    q = filterLists(q);
    o = filterLists(o);

    if (LOG.isDebugEnabled()) {
      LOG.debug("update(" + q + ", " + o + ", " + upsert + ", " + multi + ")");
    }

    if (o.containsField(ID_KEY) && q.containsField(ID_KEY) && objectComparator.compare(o.get(ID_KEY), q.get(ID_KEY)) != 0) {
      LOG.warn("can not change _id of a document query={}, document={}", q, o);
      throw new WriteConcernException(fongoDb.notOkErrorResult(16836, "can not change _id of a document " + ID_KEY));
    }

    int updatedDocuments = 0;
    boolean idOnlyUpdate = q.containsField(ID_KEY) && q.keySet().size() == 1;
    boolean updatedExisting = false;

    if (idOnlyUpdate && isNotUpdateCommand(o)) {
      if (!o.containsField(ID_KEY)) {
        o.put(ID_KEY, Util.clone(q.get(ID_KEY)));
      } else {
        o.put(ID_KEY, Util.clone(o.get(ID_KEY)));
      }
      @SuppressWarnings("unchecked") Iterator<DBObject> oldObjects = _idIndex.retrieveObjects(q).iterator();
      addToIndexes(Util.clone(o), oldObjects.hasNext() ? oldObjects.next() : null, concern);
      updatedDocuments++;
    } else {
      Filter filter = expressionParser.buildFilter(q);
      for (DBObject obj : filterByIndexes(q)) {
        if (filter.apply(obj)) {
          DBObject newObject = Util.clone(obj);
          updateEngine.doUpdate(newObject, o, q, false);
          // Check for uniqueness (throw MongoException if error)
          addToIndexes(newObject, obj, concern);

          updatedDocuments++;
          updatedExisting = true;

          if (!multi) {
            break;
          }
        }
      }
      if (updatedDocuments == 0 && upsert) {
        BasicDBObject newObject = createUpsertObject(q);
        fInsert(updateEngine.doUpdate(newObject, o, q, true), concern);
        
        updatedDocuments++;
        updatedExisting = false;
      }
    }
    return new WriteResult(updateResult(updatedDocuments, updatedExisting), concern);
  }


  private List idsIn(DBObject query) {
    Object idValue = query.get(ID_KEY);
    if (idValue == null || query.keySet().size() > 1) {
      return Collections.emptyList();
    } else if (idValue instanceof DBObject) {
      DBObject idDbObject = (DBObject) idValue;
      Collection inList = (Collection) idDbObject.get(QueryOperators.IN);

      // I think sorting the inputed keys is a rough
      // approximation of how mongo creates the bounds for walking
      // the index.  It has the desired affect of returning results
      // in _id index order, but feels pretty hacky.
      if (inList != null) {
        Object[] inListArray = inList.toArray(new Object[inList.size()]);
        // ids could be DBObjects, so we need a comparator that can handle that
        Arrays.sort(inListArray, objectComparator);
        return Arrays.asList(inListArray);
      }
      if (!isNotUpdateCommand(idValue)) {
        return Collections.emptyList();
      }
    }
    return Collections.singletonList(Util.clone(idValue));
  }

  protected BasicDBObject createUpsertObject(DBObject q) {
    BasicDBObject newObject = new BasicDBObject();
    newObject.markAsPartialObject();
    List idsIn = idsIn(q);

    if (!idsIn.isEmpty()) {
      newObject.put(ID_KEY, Util.clone(idsIn.get(0)));
    } else {
      BasicDBObject filteredQuery = new BasicDBObject();
      for (String key : q.keySet()) {
        Object value = q.get(key);
        if (isNotUpdateCommand(value)) {
          filteredQuery.put(key, value);
        }
      }
      updateEngine.mergeEmbeddedValueFromQuery(newObject, filteredQuery);
    }
    return newObject;
  }

  public boolean isNotUpdateCommand(Object value) {
    boolean okValue = true;
    if (value instanceof DBObject) {
      for (String innerKey : ((DBObject) value).keySet()) {
        if (innerKey.startsWith("$")) {
          okValue = false;
        }
      }
    }
    return okValue;
  }

  @Override
  protected void doapply(DBObject o) {
  }

  @Override
  public synchronized WriteResult remove(DBObject o, WriteConcern concern, DBEncoder encoder) throws MongoException {
    o = filterLists(o);
    if (LOG.isDebugEnabled()) {
      LOG.debug("remove: " + o);
    }
    int updatedDocuments = 0;
    Collection<DBObject> objectsByIndex = filterByIndexes(o);
    Filter filter = expressionParser.buildFilter(o);
    List<DBObject> ids = new ArrayList<DBObject>();
    // Double pass, objectsByIndex can be not "objects"
    for (DBObject object : objectsByIndex) {
      if (filter.apply(object)) {
        ids.add(object);
      }
    }
    // Real remove.
    for (DBObject object : ids) {
      LOG.debug("remove object : {}", object);
      removeFromIndexes(object);
      updatedDocuments++;
    }
    return new WriteResult(updateResult(updatedDocuments, false), concern);
  }

  @Override
  QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPref, DBDecoder decoder) {
    return find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder, null);
  }

  @Override
  synchronized QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {
    final Iterator<DBObject> values = __find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder, encoder);
    return createQueryResultIterator(values);
  }

  @Override
  public synchronized void createIndex(DBObject keys, DBObject options, DBEncoder encoder) throws MongoException {
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    BasicDBObject rec = new BasicDBObject();
    rec.append("v", 1);
    rec.append("key", keys);
    rec.append("ns", this.getDB().getName() + "." + this.getName());
    if (options != null && options.containsField("name")) {
      rec.append("name", options.get("name"));
    } else {
      StringBuilder sb = new StringBuilder();
      boolean firstLoop = true;
      for (String keyName : keys.keySet()) {
        if (!firstLoop) {
          sb.append("_");
        }
        sb.append(keyName).append("_").append(keys.get(keyName));
        firstLoop = false;
      }
      rec.append("name", sb.toString());
    }
    // Ensure index doesn't exist.
    if (indexColl.findOne(rec) != null) {
      return;
    }

    // Unique index must not be in previous find.
    boolean unique = options != null && options.get("unique") != null && (Boolean.TRUE.equals(options.get("unique")) || "1".equals(options.get("unique")) || Integer.valueOf(1).equals(options.get("unique")));
    if (unique) {
      rec.append("unique", unique);
    }
    rec.putAll(options);

    try {
      IndexAbstract index = IndexFactory.create((String) rec.get("name"), keys, unique);
      @SuppressWarnings("unchecked") List<List<Object>> notUnique = index.addAll(_idIndex.values());
      if (!notUnique.isEmpty()) {
        // Duplicate key.
        if (enforceDuplicates(getWriteConcern())) {
          fongoDb.errorResult(11000, "E11000 duplicate key error index: " + getFullName() + ".$" + rec.get("name") + "  dup key: { : " + notUnique + " }").throwOnError();
        }
        return;
      }
      indexes.add(index);
    } catch (MongoException me) {
      fongoDb.errorResult(me.getCode(), me.getMessage()).throwOnError();
    }

    // Add index if all fine.
    indexColl.insert(rec);
  }

  @Override
  public DBObject findOne(DBObject query, DBObject fields, DBObject orderBy, ReadPreference readPref) {
    QueryOpBuilder queryOpBuilder = new QueryOpBuilder().addQuery(query).addOrderBy(orderBy);
    Iterator<DBObject> resultIterator = __find(queryOpBuilder.get(), fields, 0, 1, -1, 0, readPref, null);
    return resultIterator.hasNext() ? replaceWithObjectClass(resultIterator.next()) : null;
  }

  /**
   * Used for older compatibility.
   * <p/>
   * note: encoder, decoder, readPref, options are ignored
   */
  Iterator<DBObject> __find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                            ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {
    return __find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder);
  }

  /**
   * Used for older compatibility.
   * <p/>
   * note: decoder, readPref, options are ignored
   */
  synchronized Iterator<DBObject> __find(final DBObject pRef, DBObject fields, int numToSkip, int batchSize, int limit,
                                         int options,
                                         ReadPreference readPref, DBDecoder decoder) throws MongoException {
    DBObject ref = filterLists(pRef);
    long maxScan = Long.MAX_VALUE;
    if (LOG.isDebugEnabled()) {
      LOG.debug("find({}, {}).skip({}).limit({})", ref, fields, numToSkip, limit);
      LOG.debug("the db {} looks like {}", this.getDB().getName(), _idIndex.size());
    }

    DBObject orderby = null;
    if (ref.containsField("$orderby")) {
      orderby = (DBObject) ref.get("$orderby");
    }
    if (ref.containsField("$maxScan")) {
      maxScan = ((Number) ref.get("$maxScan")).longValue();
    }
    if (ref.containsField("$query")) {
      ref = (DBObject) ref.get("$query");
    }

    Filter filter = expressionParser.buildFilter(ref);
    int foundCount = 0;
    int upperLimit = Integer.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }

    Collection<DBObject> objectsFromIndex = filterByIndexes(ref);
    List<DBObject> results = new ArrayList<DBObject>();
    List objects = idsIn(ref);
    if (!objects.isEmpty()) {
      if (!(ref.get(ID_KEY) instanceof DBObject)) {
        // Special case : find({id:<val}) doesn't handle skip...
        // But : find({_id:{$in:[1,2,3]}).skip(3) will return empty list.
        numToSkip = 0;
      }
      if (orderby == null) {
        orderby = new BasicDBObject(ID_KEY, 1);
      } else {
        // Special case : if order by is wrong (field doesn't exist), the sort must be directed by _id.
        objectsFromIndex = sortObjects(new BasicDBObject(ID_KEY, 1), objectsFromIndex);
      }
    }
    int seen = 0;
    Iterable<DBObject> objectsToSearch = sortObjects(orderby, objectsFromIndex);
    for (Iterator<DBObject> iter = objectsToSearch.iterator(); iter.hasNext() && foundCount <= upperLimit && maxScan-- > 0; ) {
      DBObject dbo = iter.next();
      if (filter.apply(dbo)) {
        if (seen++ >= numToSkip) {
          foundCount++;
          DBObject clonedDbo = Util.clone(dbo);
          if (nonIdCollection) {
            clonedDbo.removeField(ID_KEY);
          }
          for (String key : clonedDbo.keySet()) {
            Object value = clonedDbo.get(key);
            if (value instanceof DBRef && ((DBRef) value).getDB() == null) {
              clonedDbo.put(key, new DBRef(this.getDB(), ((DBRef) value).getRef(), ((DBRef) value).getId()));
            }
          }
          results.add(clonedDbo);
        }
      }
    }

    if (fields != null && !fields.keySet().isEmpty()) {
      LOG.debug("applying projections {}", fields);
      results = applyProjections(results, fields);
    }

    LOG.debug("found results {}", results);

    return replaceWithObjectClass(results).iterator();
  }

  /**
   * Return "objects.values()" if no index found.
   *
   * @return objects from "_id" if no index found, elsewhere the restricted values from an index.
   */
  private Collection<DBObject> filterByIndexes(DBObject ref) {
    Collection<DBObject> dbObjectIterable = null;
    if (ref != null) {
      IndexAbstract matchingIndex = searchIndex(ref);
      if (matchingIndex != null) {
        //noinspection unchecked
        dbObjectIterable = matchingIndex.retrieveObjects(ref);
        if (LOG.isDebugEnabled()) {
          LOG.debug("restrict with index {}, from {} to {} elements", matchingIndex.getName(), _idIndex.size(), dbObjectIterable == null ? 0 : dbObjectIterable.size());
        }
      }
    }
    if (dbObjectIterable == null) {
      //noinspection unchecked
      dbObjectIterable = _idIndex.values();
    }
    return dbObjectIterable;
  }

  private List<DBObject> applyProjections(List<DBObject> results, DBObject projection) {
    final List<DBObject> ret = new ArrayList<DBObject>(results.size());

    for (DBObject result : results) {
      DBObject projectionMacthedResult = applyProjections(result, projection);
      if (null != projectionMacthedResult) {
        ret.add(projectionMacthedResult);
      }
    }

    return ret;
  }


  private static void addValuesAtPath(BasicDBObject ret, DBObject dbo, List<String> path, int startIndex) {
    String subKey = path.get(startIndex);
    Object value = dbo.get(subKey);

    if (path.size() > startIndex + 1) {
      if (value instanceof DBObject && !(value instanceof List)) {
        BasicDBObject nb = (BasicDBObject) ret.get(subKey);
        if (nb == null) {
          nb = new BasicDBObject();
        }
        ret.append(subKey, nb);
        addValuesAtPath(nb, (DBObject) value, path, startIndex + 1);
      } else if (value instanceof List) {

        BasicDBList list = getListForKey(ret, subKey);

        int idx = 0;
        for (Object v : (List) value) {
          if (v instanceof DBObject) {
            BasicDBObject nb;
            if (list.size() > idx) {
              nb = (BasicDBObject) list.get(idx);
            } else {
              nb = new BasicDBObject();
              list.add(nb);
            }
            addValuesAtPath(nb, (DBObject) v, path, startIndex + 1);
          }
          idx++;
        }
      }
    } else if (value != null) {
      ret.append(subKey, value);
    }
  }

  private static BasicDBList getListForKey(BasicDBObject ret, String subKey) {
    BasicDBList list;
    if (ret.containsField(subKey)) {
      list = (BasicDBList) ret.get(subKey);
    } else {
      list = new BasicDBList();
      ret.append(subKey, list);
    }
    return list;
  }

  /**
   * Replaces the result {@link DBObject} with the configured object class of this collection. If the object class is
   * <code>null</code> the result object itself will be returned.
   * 
   * @param resultObject
   *          the original result value from the command.
   * @return replaced {@link DBObject} if necessary, or resultObject.
   */
  private DBObject replaceWithObjectClass(DBObject resultObject) {
    if (resultObject == null || getObjectClass() == null) {
      return resultObject;
    }

    final DBObject targetObject = instantiateObjectClassInstance();

    for (final String key : resultObject.keySet()) {
      targetObject.put(key, resultObject.get(key));
    }

    return targetObject;
  }
  
  private List<DBObject> replaceWithObjectClass(List<DBObject> resultObjects) {
    
    final List<DBObject> targetObjects = new ArrayList<DBObject>(resultObjects.size());
    
    for(final DBObject resultObject : resultObjects)
    {
      targetObjects.add(replaceWithObjectClass(resultObject));
    }
    
    return targetObjects;
  }

  /**
   * Returns a new instance of the object class.
   * @return a new instance of the object class.
   */
  private DBObject instantiateObjectClassInstance() {
    try {
      return (DBObject) getObjectClass().newInstance();
    } catch (InstantiationException e) {
      throw new MongoInternalException("Can't create instance of type: " + getObjectClass(), e);
    } catch (IllegalAccessException e) {
      throw new MongoInternalException("Can't create instance of type: " + getObjectClass(), e);
    }
  }

  /**
   * Applies the requested <a href="http://docs.mongodb.org/manual/core/read-operations/#result-projections">projections</a> to the given object.
   * TODO: Support for projection operators: http://docs.mongodb.org/manual/reference/operator/projection/
   */
  public static DBObject applyProjections(DBObject result, DBObject projectionObject) {
    if (projectionObject == null) {
      return Util.cloneIdFirst(result);
    }

    if (result == null) {
      return null; // #35
    }

    int inclusionCount = 0;
    int exclusionCount = 0;
    List<String> projectionFields = new ArrayList<String>();

    boolean wasIdExcluded = false;
    List<Tuple2<List<String>, Boolean>> projections = new ArrayList<Tuple2<List<String>, Boolean>>();
    for (String projectionKey : projectionObject.keySet()) {
      final Object projectionValue = projectionObject.get(projectionKey);
      boolean included = false;
      boolean project = false;
      if (projectionValue instanceof Number) {
        included = ((Number) projectionValue).intValue() > 0;
      } else if (projectionValue instanceof Boolean) {
        included = (Boolean) projectionValue;
      } else if (projectionValue instanceof DBObject) {
        project = true;
        projectionFields.add(projectionKey);
      } else if (!projectionValue.toString().equals("text")) {
        final String msg = "Projection `" + projectionKey
            + "' has a value that Fongo doesn't know how to handle: " + projectionValue
            + " (" + (projectionValue == null ? " " : projectionValue.getClass() + ")");

        throw new IllegalArgumentException(msg);
      }
      List<String> projectionPath = Util.split(projectionKey);

      if (!ID_KEY.equals(projectionKey)) {
        if (included) {
          inclusionCount++;
        } else if (!project) {
          exclusionCount++;
        }
      } else {
        wasIdExcluded = !included;
      }
      if (projectionPath.size() > 0) {
        projections.add(new Tuple2<List<String>, Boolean>(projectionPath, included));
      }
    }

    if (inclusionCount > 0 && exclusionCount > 0) {
      throw new IllegalArgumentException(
          "You cannot combine inclusion and exclusion semantics in a single projection with the exception of the _id field: "
              + projectionObject
      );
    }

    BasicDBObject ret;
    if (exclusionCount > 0) {
      ret = (BasicDBObject) Util.clone(result);
    } else {
      ret = new BasicDBObject();
      if (!wasIdExcluded) {
        ret.append(ID_KEY, Util.clone(result.get(ID_KEY)));
      } else if (inclusionCount == 0) {
        ret = (BasicDBObject) Util.clone(result);
        ret.removeField(ID_KEY);
      }
    }

    for (Tuple2<List<String>, Boolean> projection : projections) {
      if (projection._1.size() == 1 && !projection._2) {
        ret.removeField(projection._1.get(0));
      } else {
        addValuesAtPath(ret, result, projection._1, 0);
      }
    }

    if (!projectionFields.isEmpty()) {
      for (String projectionKey : projectionObject.keySet()) {
        if (!projectionFields.contains(projectionKey)) {
          continue;
        }
        final Object projectionValue = projectionObject.get(projectionKey);
        final boolean isElemMatch =
            ((BasicDBObject) projectionObject.get(projectionKey)).containsField(QueryOperators.ELEM_MATCH);
        if (isElemMatch) {
          ret.removeField(projectionKey);
          List searchIn = ((BasicDBList) result.get(projectionKey));
          DBObject searchFor =
              (BasicDBObject) ((BasicDBObject) projectionObject.get(projectionKey)).get(QueryOperators.ELEM_MATCH);
          String searchKey = (String) searchFor.keySet().toArray()[0];
          int pos = -1;
          for (int i = 0; i < searchIn.size(); i++) {
            boolean matches;
            DBObject fieldToSearch = (BasicDBObject) searchIn.get(i);
            if (fieldToSearch.containsField(searchKey)) {
              if (searchFor.get(searchKey) instanceof ObjectId
                  && fieldToSearch.get(searchKey) instanceof String) {
                ObjectId m1 = new ObjectId(searchFor.get(searchKey).toString());
                ObjectId m2 = new ObjectId(String.valueOf(fieldToSearch.get(searchKey)));
                matches = m1.equals(m2);
              } else if (searchFor.get(searchKey) instanceof String
                  && fieldToSearch.get(searchKey) instanceof ObjectId) {
                ObjectId m1 = new ObjectId(String.valueOf(searchFor.get(searchKey)));
                ObjectId m2 = new ObjectId(fieldToSearch.get(searchKey).toString());
                matches = m1.equals(m2);
              } else {
                matches = fieldToSearch.get(searchKey).equals(searchFor.get(searchKey));
              }
              if (matches) {
                pos = i;
                break;
              }
            }
          }
          if (pos != -1) {
            BasicDBList append = new BasicDBList();
            append.add(searchIn.get(pos));
            ret.append(projectionKey, append);
            LOG.debug("$elemMatch projection of field \"{}\", gave result: {} ({})", projectionKey, ret, ret.getClass());
          }
        } else {
          final String msg = "Projection `" + projectionKey
              + "' has a value that Fongo doesn't know how to handle: " + projectionValue
              + " (" + (projectionValue == null ? " " : projectionValue.getClass() + ")");

          throw new IllegalArgumentException(msg);
        }
      }
    }

    return ret;
  }

  public Collection<DBObject> sortObjects(final DBObject orderby, final Collection<DBObject> objects) {
    Collection<DBObject> objectsToSearch = objects;
    if (orderby != null) {
      final Set<String> orderbyKeySet = orderby.keySet();
      if (!orderbyKeySet.isEmpty()) {
        DBObject[] objectsToSort = objects.toArray(new DBObject[objects.size()]);

        Arrays.sort(objectsToSort, new Comparator<DBObject>() {
          @Override
          public int compare(DBObject o1, DBObject o2) {
            for (String sortKey : orderbyKeySet) {
              final List<String> path = Util.split(sortKey);
              int sortDirection = (Integer) orderby.get(sortKey);

              List<Object> o1list = expressionParser.getEmbeddedValues(path, o1);
              List<Object> o2list = expressionParser.getEmbeddedValues(path, o2);

              int compareValue = expressionParser.compareLists(o1list, o2list) * sortDirection;
              if (compareValue != 0) {
                return compareValue;
              }
            }
            return 0;
          }
        });
        objectsToSearch = Arrays.asList(objectsToSort);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("sorted objectsToSearch " + objectsToSearch);
    }
    return objectsToSearch;
  }


  @Override
  public synchronized long getCount(DBObject query, DBObject fields, long limit, long skip) {
    query = filterLists(query);
    Filter filter = query == null ? ExpressionParser.AllFilter : expressionParser.buildFilter(query);
    long count = 0;
    long upperLimit = Long.MAX_VALUE;
    if (limit > 0) {
      upperLimit = limit;
    }
    int seen = 0;
    for (Iterator<DBObject> iter = filterByIndexes(query).iterator(); iter.hasNext() && count <= upperLimit; ) {
      DBObject value = iter.next();
      if (filter.apply(value)) {
        if (seen++ >= skip) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public synchronized long getCount(DBObject query, DBObject fields, ReadPreference readPrefs) {
    //as we're in memory we don't need to worry about readPrefs
    return getCount(query, fields, 0, 0);
  }

  @Override
  public synchronized DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert) {
    LOG.debug("findAndModify({}, {}, {}, {}, {}, {}, {}", query, fields, sort, remove, update, returnNew, upsert);
    query = filterLists(query);
    update = filterLists(update);
    Filter filter = expressionParser.buildFilter(query);

    Iterable<DBObject> objectsToSearch = sortObjects(sort, filterByIndexes(query));
    DBObject beforeObject = null;
    DBObject afterObject = null;
    for (DBObject dbo : objectsToSearch) {
      if (filter.apply(dbo)) {
        beforeObject = dbo;
        if (!remove) {
          afterObject = Util.clone(beforeObject);
          updateEngine.doUpdate(afterObject, update, query, false);
          addToIndexes(afterObject, beforeObject, getWriteConcern());
          break;
        } else {
          remove(dbo);
          return dbo;
        }
      }
    }
    if (beforeObject != null && !returnNew) {
      return replaceWithObjectClass(applyProjections(beforeObject, fields));
    }
    if (beforeObject == null && upsert && !remove) {
      beforeObject = new BasicDBObject();
      afterObject = createUpsertObject(query);
      fInsert(updateEngine.doUpdate(afterObject, update, query, upsert), getWriteConcern());
    }

    final DBObject resultObject;
    if (returnNew) {
      resultObject = applyProjections(afterObject, fields);
    } else {
      resultObject = applyProjections(beforeObject, fields);
    }

    return replaceWithObjectClass(resultObject);
  }

  @Override
  public synchronized List distinct(String key, DBObject query) {
    query = filterLists(query);
    Set<Object> results = new LinkedHashSet<Object>();
    Filter filter = expressionParser.buildFilter(query);
    for (Iterator<DBObject> iter = filterByIndexes(query).iterator(); iter.hasNext(); ) {
      DBObject value = iter.next();
      if (filter.apply(value)) {
        List<Object> keyValues = expressionParser.getEmbeddedValues(key, value);
        for (Object keyValue : keyValues) {
          if (keyValue instanceof List) {
            results.addAll((List) keyValue);
          } else {
            results.add(keyValue);
          }
        }
      }
    }
    //noinspection unchecked
    return new ArrayList(results);
  }

  @Override
  public Cursor aggregate(List<DBObject> pipeline, AggregationOptions options, ReadPreference readPreference) {
    return this.createQueryResultIterator(this.aggregate(pipeline, readPreference).results().iterator());
  }

  @Override
  public List<Cursor> parallelScan(ParallelScanOptions options) {
    return Arrays.asList((Cursor) this.createQueryResultIterator(this._idIndex.values().iterator()));
  }

  @Override
  BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> requests, WriteConcern writeConcern, DBEncoder encoder) {
    isTrue("no operations", !requests.isEmpty());
    // TODO
    return null;
  }

  protected synchronized void _dropIndexes(String name) throws MongoException {
    DBCollection indexColl = fongoDb.getCollection("system.indexes");
    indexColl.remove(new BasicDBObject("name", name));
    ListIterator<IndexAbstract> iterator = indexes.listIterator();
    while (iterator.hasNext()) {
      IndexAbstract index = iterator.next();
      if (index.getName().equals(name)) {
        iterator.remove();
        break;
      }
    }
  }

  protected synchronized void _dropIndexes() {
    List<DBObject> indexes = fongoDb.getCollection("system.indexes").find().toArray();
    // Two step for no concurrent modification exception
    for (DBObject index : indexes) {
      if (!ID_NAME_INDEX.equals(index.get("name").toString())) {
        dropIndexes(index.get("name").toString());
      }
    }
  }

  @Override
  public void drop() {
    _idIndex.clear();
    _dropIndexes(); // _idIndex must stay.
    fongoDb.removeCollection(this);
  }

  /**
   * Search the most restrictive index for query.
   *
   * @param query query for restriction
   * @return the most restrictive index, or null.
   */
  private synchronized IndexAbstract searchIndex(DBObject query) {
    IndexAbstract result = null;
    int foundCommon = -1;
    Set<String> queryFields = query.keySet();
    for (IndexAbstract index : indexes) {
      if (index.canHandle(queryFields)) {
        // The most restrictive first.
        if (index.getFields().size() > foundCommon || (result != null && !result.isUnique() && index.isUnique())) {
          result = index;
          foundCommon = index.getFields().size();
        }
      }
    }

    LOG.debug("searchIndex() found index {} for fields {}", result, queryFields);

    return result;
  }

  /**
   * Search the geo index.
   *
   * @return the geo index, or null.
   */
  private synchronized IndexAbstract searchGeoIndex(boolean unique) {
    IndexAbstract result = null;
    for (IndexAbstract index : indexes) {
      if (index.isGeoIndex()) {
        if (result != null && unique) {
          this.fongoDb.notOkErrorResult(-5, "more than one 2d index, not sure which to run geoNear on").throwOnError();
        }
        result = index;
        if (!unique) {
          break;
        }
      }
    }

    LOG.debug("searchGeoIndex() found index {}", result);

    return result;
  }

  /**
   * Add entry to index.
   * If necessary, remove oldObject from index.
   *
   * @param object    new object to insert.
   * @param oldObject null if insert, old object if update.
   */
  private synchronized void addToIndexes(DBObject object, DBObject oldObject, WriteConcern concern) {
    // Ensure "insert/update" create collection into "fongoDB"
    this.fongoDb.addCollection(this);
    Set<String> queryFields = object.keySet();
    // First, try to see if index can add the new value.
    for (IndexAbstract index : indexes) {
      @SuppressWarnings("unchecked") List<List<Object>> error = index.checkAddOrUpdate(object, oldObject);
      if (!error.isEmpty()) {
        // TODO formatting : E11000 duplicate key error index: test.zip.$city_1_state_1_pop_1  dup key: { : "BARRE", : "MA", : 4546.0 }
        if (enforceDuplicates(concern)) {
          fongoDb.errorResult(11001, "E11000 duplicate key error index: " + this.getFullName() + "." + index.getName() + "  dup key : {" + error + " }").throwOnError();
        }
        return; // silently ignore.
      }
    }

    DBObject idFirst = Util.cloneIdFirst(object);
    Set<String> oldQueryFields = oldObject == null ? Collections.<String>emptySet() : oldObject.keySet();
    for (IndexAbstract index : indexes) {
      if (index.canHandle(queryFields)) {
        index.addOrUpdate(idFirst, oldObject);
      } else if (index.canHandle(oldQueryFields))
        // In case of update and removing a field, we must remove from the index.
        index.remove(oldObject);
    }
  }

  /**
   * Remove an object from indexes.
   *
   * @param object object to remove.
   */
  private synchronized void removeFromIndexes(DBObject object) {
    Set<String> queryFields = object.keySet();
    for (IndexAbstract index : indexes) {
      if (index.canHandle(queryFields)) {
        index.remove(object);
      }
    }
  }

  public synchronized Collection<IndexAbstract> getIndexes() {
    return Collections.unmodifiableList(indexes);
  }

  public synchronized List<DBObject> geoNear(DBObject near, DBObject query, Number limit, Number maxDistance, boolean spherical) {
    IndexAbstract matchingIndex = searchGeoIndex(true);
    if (matchingIndex == null) {
      fongoDb.notOkErrorResult(-5, "no geo indices for geoNear").throwOnError();
    }
    //noinspection ConstantConditions
    LOG.info("geoNear() near:{}, query:{}, limit:{}, maxDistance:{}, spherical:{}, use index:{}", near, query, limit, maxDistance, spherical, matchingIndex.getName());

    List<LatLong> latLongs = GeoUtil.latLon(Collections.<String>emptyList(), near);
    return ((GeoIndex) matchingIndex).geoNear(query == null ? new BasicDBObject() : query, latLongs, limit == null ? 100 : limit.intValue(), spherical);
  }

  //Text search Emulation see http://docs.mongodb.org/manual/tutorial/search-for-text/ for mongo
  public synchronized DBObject text(String search, Number limit, DBObject project) {
    TextSearch ts = new TextSearch(this);
    return ts.findByTextSearch(search, project == null ? new BasicDBObject() : project, limit == null ? 100 : limit.intValue());
  }

  private QueryResultIterator createQueryResultIterator(Iterator<DBObject> values) {
    try {
      QueryResultIterator iterator = new ObjenesisStd().getInstantiatorOf(QueryResultIterator.class).newInstance();
      Field field = QueryResultIterator.class.getDeclaredField("_cur");
      field.setAccessible(true);
      field.set(iterator, values);
      return iterator;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
