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
public class Skip extends PipelineKeyword {
  public static final Skip INSTANCE = new Skip();

  private Skip() {
  }

  /**
   * @param coll
   * @param object
   * @return
   */
  @Override
  public DBCollection apply(DBCollection coll, DBObject object) {
    List<DBObject> objects = coll.find().skip(((Number) object.get(getKeyword())).intValue()).toArray();
    return dropAndInsert(coll, objects);
  }

  @Override
  public String getKeyword() {
    return "$skip";
  }
}
