package com.github.fakemongo.junit;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import org.junit.rules.ExternalResource;

/**
 * Create a Junit Rule to use with annotation
 * <p>
 * &#64;Rule
 * public FongoRule rule = new FongoRule().
 * </p>
 * <p>
 * Note than you can switch to a real mongodb on your localhost (for now).
 * </p>
 * <p><b>
 * WARNING : database is dropped after the test !!
 * </b></P>
 */
public class FongoRule extends ExternalResource {

  /**
   * Will be true if we use the real MongoDB to test things against real world.
   */
  private final boolean realMongo;

  private final String dbName;

  private final Fongo fongo;

  private Mongo mongo;

  private DB db;

  /**
   * Setup a rule with a real MongoDB.
   *
   * @param dbName            the dbName to use.
   * @param realMongo         set to true if you want to use a real mongoDB.
   * @param mongoClientIfReal real client to use if realMongo si true.
   */
  public FongoRule(String dbName, boolean realMongo, MongoClient mongoClientIfReal) {
    this.dbName = dbName;
    this.realMongo = realMongo;
    this.fongo = realMongo ? null : newFongo();
    this.mongo = mongoClientIfReal;
  }

  public FongoRule() {
    this(UUID.randomUUID().toString(), false, null);
  }

  public FongoRule(boolean realMongo) {
    this(UUID.randomUUID().toString(), realMongo, null);
  }

  public FongoRule(boolean realMongo, MongoClient mongoClientIfReal) {
    this(UUID.randomUUID().toString(), realMongo, mongoClientIfReal);
  }

  public FongoRule(String dbName, boolean realMongo) {
    this(dbName, realMongo, null);
  }

  public FongoRule(String dbName) {
    this(dbName, false, null);
  }

  public boolean isRealMongo() {
    return this.realMongo;
  }

  @Override
  protected void before() throws UnknownHostException {
    if (realMongo) {
      if (mongo == null) {
        mongo = new MongoClient();
      }
    } else {
      mongo = this.fongo.getMongo();
    }
    db = mongo.getDB(dbName);
  }

  @Override
  protected void after() {
    db.dropDatabase();
  }

  public DBCollection insertJSON(DBCollection coll, String json) {
    List<DBObject> objects = parseList(json);
    for (DBObject object : objects) {
      coll.insert(object);
    }
    return coll;
  }

  public DBCollection insertFile(DBCollection coll, String filename) throws IOException {
    InputStream is = this.getClass().getResourceAsStream(filename);
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line = br.readLine();
      while (line != null) {
        coll.insert(this.parseDBObject(line));
        line = br.readLine();
      }
    } finally {
      if (is != null) {
        is.close();
      }
    }
    return coll;
  }

  public List<DBObject> parseList(String json) {
    return parse(json);
  }

  public DBObject parseDBObject(String json) {
    return parse(json);
  }

  @SuppressWarnings("unchecked")
  public <T> T parse(String json) {
    return (T) JSON.parse(json);
  }

  public DBCollection newCollection() {
    return newCollection(UUID.randomUUID().toString());
  }

  public DBCollection newCollection(String collectionName) {
    return db.getCollection(collectionName);
  }

  private Fongo newFongo() {
    return new Fongo("test");
  }

  public Fongo getFongo() {
    return this.fongo;
  }

  @Deprecated
  public DB getDb() {
    return this.db;
  }

  @Deprecated
  public DB getDb(String name) {
    return this.mongo.getDB(name);
  }

  public DB getDB() {
    return this.db;
  }

  public DB getDB(String name) {
    return this.mongo.getDB(name);
  }

  public Mongo getMongo() {
    return this.mongo;
  }

}
