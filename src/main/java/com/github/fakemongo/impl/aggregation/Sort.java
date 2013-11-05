package com.github.fakemongo.impl.aggregation;

import java.util.List;

import org.bson.util.annotations.ThreadSafe;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * User: william
 * Date: 24/07/13
 */
@ThreadSafe
public class Sort extends PipelineKeyword {
  public static final Sort INSTANCE = new Sort();

  private Sort() {
  }

  /**
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    List<DBObject> objects = coll.find().sort((DBObject) object.get(getKeyword())).toArray();
    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$sort";
  }

}
