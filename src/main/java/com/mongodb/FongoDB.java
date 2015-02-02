package com.mongodb;

import com.github.fakemongo.Fongo;
import com.github.fakemongo.impl.Aggregator;
import com.github.fakemongo.impl.MapReduce;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * fongo override of com.mongodb.DB
 * you shouldn't need to use this class directly
 *
 * @author jon
 */
public class FongoDB extends DB {
  private final static Logger LOG = LoggerFactory.getLogger(FongoDB.class);
  public static final String SYSTEM_NAMESPACES = "system.namespaces";

  private final Map<String, FongoDBCollection> collMap = Collections.synchronizedMap(new HashMap<String, FongoDBCollection>());
  private final Set<String> namespaceDeclarated = Collections.synchronizedSet(new LinkedHashSet<String>());
  private final Fongo fongo;

  private MongoCredential mongoCredential;

  public FongoDB(Fongo fongo, String name) {
    super(fongo.getMongo(), name);
    this.fongo = fongo;
    doGetCollection("system.users");
    doGetCollection("system.indexes");
    doGetCollection(SYSTEM_NAMESPACES);
  }

  @Override
  public void requestStart() {
  }

  @Override
  public void requestDone() {
  }

  @Override
  public void requestEnsureConnection() {
  }

  @Override
  protected FongoDBCollection doGetCollection(String name) {
    synchronized (collMap) {
      FongoDBCollection coll = collMap.get(name);
      if (coll == null) {
        coll = new FongoDBCollection(this, name);
        collMap.put(name, coll);
      }
      return coll;
    }
  }

  private DBObject findAndModify(String collection, DBObject query, DBObject sort, boolean remove, DBObject update, boolean returnNew, DBObject fields, boolean upsert) {
    FongoDBCollection coll = doGetCollection(collection);

    return coll.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
  }

  private List<DBObject> doAggregateCollection(String collection, List<DBObject> pipeline) {
    FongoDBCollection coll = doGetCollection(collection);
    Aggregator aggregator = new Aggregator(this, coll, pipeline);

    return aggregator.computeResult();
  }

  private DBObject doMapReduce(String collection, String map, String reduce, String finalize, DBObject out, DBObject query, DBObject sort, Number limit) {
    FongoDBCollection coll = doGetCollection(collection);
    MapReduce mapReduce = new MapReduce(this.fongo, coll, map, reduce, finalize, out, query, sort, limit);
    return mapReduce.computeResult();
  }

  private List<DBObject> doGeoNearCollection(String collection, DBObject near, DBObject query, Number limit, Number maxDistance, boolean spherical) {
    FongoDBCollection coll = doGetCollection(collection);
    return coll.geoNear(near, query, limit, maxDistance, spherical);
  }

  //see http://docs.mongodb.org/manual/tutorial/search-for-text/ for mongodb
  private DBObject doTextSearchInCollection(String collection, String search, Integer limit, DBObject project) {
    FongoDBCollection coll = doGetCollection(collection);
    return coll.text(search, limit, project);
  }

//  @Override
//  public Set<String> getCollectionNames() throws MongoException {
//    Set<String> names = new HashSet<String>();
//    for (FongoDBCollection fongoDBCollection : collMap.values()) {
//      int expectedCount = 0;
//      if (fongoDBCollection.getName().startsWith("system.indexes")) {
//        expectedCount = 1;
//      }
//
//      if (fongoDBCollection.count() > expectedCount) {
//        names.add(fongoDBCollection.getName());
//      }
//    }
//
//    return names;
//  }

  @Override
  public void cleanCursors(boolean force) throws MongoException {
  }

  @Override
  public DB getSisterDB(String name) {
    return fongo.getDB(name);
  }

  @Override
  public WriteConcern getWriteConcern() {
    return fongo.getWriteConcern();
  }

  @Override
  public ReadPreference getReadPreference() {
    return ReadPreference.primaryPreferred();
  }

  @Override
  public void dropDatabase() throws MongoException {
    this.fongo.dropDatabase(this.getName());
    for (FongoDBCollection c : new ArrayList<FongoDBCollection>(collMap.values())) {
      c.drop();
    }
  }

  @Override
  CommandResult doAuthenticate(MongoCredential credentials) {
    this.mongoCredential = credentials;
    return okResult();
  }

  @Override
  MongoCredential getAuthenticationCredentials() {
    return this.mongoCredential;
  }

  /**
   * Executes a database command.
   *
   * @param cmd       dbobject representing the command to execute
   * @param options   query options to use
   * @param readPrefs ReadPreferences for this command (nodes selection is the biggest part of this)
   * @return result of command from the database
   * @throws MongoException
   * @dochub commands
   * @see <a href="http://mongodb.onconfluence.com/display/DOCS/List+of+Database+Commands">List of Commands</a>
   */
  @Override
  public CommandResult command(DBObject cmd, int options, ReadPreference readPrefs) throws MongoException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fongo got command " + cmd);
    }
    if (cmd.containsField("getlasterror") || cmd.containsField("getLastError")) {
      return okResult();
    } else if (cmd.containsField("fsync")) {
      return okResult();
    } else if (cmd.containsField("drop")) {
      this.getCollection(cmd.get("drop").toString()).drop();
      return okResult();
    } else if (cmd.containsField("create")) {
      String collectionName = (String) cmd.get("create");
      doGetCollection(collectionName);
      return okResult();
    } else if (cmd.containsField("count")) {
      String collectionName = (String) cmd.get("count");
      Number limit = (Number) cmd.get("limit");
      Number skip = (Number) cmd.get("skip");
      long result = doGetCollection(collectionName).getCount(
          (DBObject) cmd.get("query"),
          null,
          limit == null ? 0L : limit.longValue(),
          skip == null ? 0L : skip.longValue());
      CommandResult okResult = okResult();
      okResult.append("n", (double) result);
      return okResult;
    } else if (cmd.containsField("deleteIndexes")) {
      String collectionName = (String) cmd.get("deleteIndexes");
      String indexName = (String) cmd.get("index");
      if ("*".equals(indexName)) {
        doGetCollection(collectionName)._dropIndexes();
      } else {
        doGetCollection(collectionName)._dropIndex(indexName);
      }
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("aggregate")) {
      @SuppressWarnings(
          "unchecked") List<DBObject> result = doAggregateCollection((String) cmd.get("aggregate"), (List<DBObject>) cmd.get("pipeline"));
      if (result == null) {
        return notOkErrorResult("can't aggregate");
      }
      CommandResult okResult = okResult();
      BasicDBList list = new BasicDBList();
      list.addAll(result);
      okResult.put("result", list);
      return okResult;
    } else if (cmd.containsField("findAndModify")) {
      return runFindAndModify(cmd, "findAndModify");
    } else if (cmd.containsField("findandmodify")) {
      return runFindAndModify(cmd, "findandmodify");
    } else if (cmd.containsField("ping")) {
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("validate")) {
      CommandResult okResult = okResult();
      return okResult;
    } else if (cmd.containsField("buildInfo") || cmd.containsField("buildinfo")) {
      CommandResult okResult = okResult();
      okResult.put("version", "2.4.5");
      okResult.put("maxBsonObjectSize", 16777216);
      return okResult;
    } else if (cmd.containsField("forceerror")) {
      // http://docs.mongodb.org/manual/reference/command/forceerror/
      CommandResult result = notOkErrorResult(10038, null, "exception: forced error");
      return result;
    } else if (cmd.containsField("mapreduce")) {
      return runMapReduce(cmd, "mapreduce");
    } else if (cmd.containsField("mapReduce")) {
      return runMapReduce(cmd, "mapReduce");
    } else if (cmd.containsField("geoNear")) {
      // http://docs.mongodb.org/manual/reference/command/geoNear/
      // TODO : handle "num" (override limit)
      try {
        List<DBObject> result = doGeoNearCollection((String) cmd.get("geoNear"),
            (DBObject) cmd.get("near"),
            (DBObject) cmd.get("query"),
            (Number) cmd.get("limit"),
            (Number) cmd.get("maxDistance"),
            Boolean.TRUE.equals(cmd.get("spherical")));
        if (result == null) {
          return notOkErrorResult("can't geoNear");
        }
        CommandResult okResult = okResult();
        BasicDBList list = new BasicDBList();
        list.addAll(result);
        okResult.put("results", list);
        return okResult;
      } catch (MongoException me) {
        CommandResult result = errorResult(me.getCode(), me.getMessage());
        return result;
      }
    } else {
      String collectionName = ((Map.Entry<String, DBObject>) cmd.toMap().entrySet().iterator().next()).getKey();
      if (collectionExists(collectionName)) {
        DBObject newCmd = (DBObject) cmd.get(collectionName);
        if ((newCmd.containsField("text") && ((DBObject) newCmd.get("text")).containsField("search"))) {
          DBObject resp = doTextSearchInCollection(collectionName,
              (String) ((DBObject) newCmd.get("text")).get("search"),
              (Integer) ((DBObject) newCmd.get("text")).get("limit"),
              (DBObject) ((DBObject) newCmd.get("text")).get("project"));
          if (resp == null) {
            return notOkErrorResult("can't perform text search");
          }
          CommandResult okResult = okResult();
          okResult.put("results", resp.get("results"));
          okResult.put("stats", resp.get("stats"));
          return okResult;
        } else if ((newCmd.containsField("$text") && ((DBObject) newCmd.get("$text")).containsField("$search"))) {
          DBObject resp = doTextSearchInCollection(collectionName,
              (String) ((DBObject) newCmd.get("$text")).get("$search"),
              (Integer) ((DBObject) newCmd.get("text")).get("limit"),
              (DBObject) ((DBObject) newCmd.get("text")).get("project"));
          if (resp == null) {
            return notOkErrorResult("can't perform text search");
          }
          CommandResult okResult = okResult();
          okResult.put("results", resp.get("results"));
          okResult.put("stats", resp.get("stats"));
          return okResult;
        }
      }
    }
    String command = cmd.toString();
    if (!cmd.keySet().isEmpty()) {
      command = cmd.keySet().iterator().next();
    }
    return notOkErrorResult(null, "no such cmd: " + command);
  }

  /**
   * Returns a set containing the names of all collections in this database.
   *
   * @return the names of collections in this database
   * @throws com.mongodb.MongoException
   * @mongodb.driver.manual reference/method/db.getCollectionNames/ getCollectionNames()
   */
//  @Override
  public Set<String> getCollectionNames() {
    List<String> collectionNames = new ArrayList<String>();
    Iterator<DBObject> collections = getCollection("system.namespaces")
        .find(new BasicDBObject(), null, 0, 0, 0, getOptions(), ReadPreference.primary(), null);
    for (; collections.hasNext(); ) {
      String collectionName = collections.next().get("name").toString();
      if (!collectionName.contains("$")) {
        collectionNames.add(collectionName.substring(getName().length() + 1));
      }
    }

    Collections.sort(collectionNames);
    return new LinkedHashSet<String>(collectionNames);
  }

  public CommandResult okResult() {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", 1.0);
    return result;
  }

  public CommandResult okErrorResult(int code, String err) {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", 1.0);
    result.put("code", code);
    if (err != null) {
      result.put("err", err);
    }
    return result;
  }

  public CommandResult notOkErrorResult(String err) {
    return notOkErrorResult(err, null);
  }

  public CommandResult notOkErrorResult(String err, String errmsg) {
    CommandResult result = new CommandResult(fongo.getServerAddress());
    result.put("ok", 0);
    if (err != null) {
      result.put("err", err);
    }
    if (errmsg != null) {
      result.put("errmsg", errmsg);
    }
    return result;
  }

  public CommandResult notOkErrorResult(int code, String err) {
    CommandResult result = notOkErrorResult(err);
    result.put("code", code);
    return result;
  }

  public CommandResult notOkErrorResult(int code, String err, String errmsg) {
    CommandResult result = notOkErrorResult(err, errmsg);
    result.put("code", code);
    return result;
  }

  public CommandResult errorResult(int code, String err) {
    CommandResult result = okResult();
    result.put("err", err);
    result.put("code", code);
    result.put("ok", false);
    return result;
  }

  @Override
  public String toString() {
    return "FongoDB." + this.getName();
  }

  public synchronized void removeCollection(FongoDBCollection collection) {
    this.collMap.remove(collection.getName());
    this.getCollection(SYSTEM_NAMESPACES).remove(new BasicDBObject("name", collection.getFullName()));
    this.namespaceDeclarated.remove(collection.getFullName());
  }

  public synchronized void addCollection(FongoDBCollection collection) {
    this.collMap.put(collection.getName(), collection);
    if (!collection.getName().startsWith("system.")) {
      if (!this.namespaceDeclarated.contains(collection.getFullName())) {
        this.getCollection(SYSTEM_NAMESPACES).insert(new BasicDBObject("name", collection.getFullName()));
        if (this.namespaceDeclarated.size() == 0) {
          this.getCollection(SYSTEM_NAMESPACES).insert(new BasicDBObject("name", collection.getDB().getName() + ".system.indexes"));
        }
        this.namespaceDeclarated.add(collection.getFullName());
      }
    }
  }

  private CommandResult runFindAndModify(DBObject cmd, String key) {
    DBObject result = findAndModify(
        (String) cmd.get(key),
        (DBObject) cmd.get("query"),
        (DBObject) cmd.get("sort"),
        Boolean.TRUE.equals(cmd.get("remove")),
        (DBObject) cmd.get("update"),
        Boolean.TRUE.equals(cmd.get("new")),
        (DBObject) cmd.get("fields"),
        Boolean.TRUE.equals(cmd.get("upsert")));
    CommandResult okResult = okResult();
    okResult.put("value", result);
    return okResult;
  }

  private CommandResult runMapReduce(DBObject cmd, String key) {
    DBObject result = doMapReduce(
        (String) cmd.get(key),
        (String) cmd.get("map"),
        (String) cmd.get("reduce"),
        (String) cmd.get("finalize"),
        (DBObject) cmd.get("out"),
        (DBObject) cmd.get("query"),
        (DBObject) cmd.get("sort"),
        (Number) cmd.get("limit"));
    if (result == null) {
      return notOkErrorResult("can't mapReduce");
    }
    CommandResult okResult = okResult();
    if (result instanceof List) {
      // INLINE case.
      okResult.put("results", result);
    } else {
      okResult.put("result", result);
    }
    return okResult;
  }
}
