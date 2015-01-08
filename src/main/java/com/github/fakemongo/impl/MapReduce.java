package com.github.fakemongo.impl;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.mozilla.javascript.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
 * <p/>
 * TODO : finalize.
 */
public class MapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class);

  private final Fongo fongo;

  private final FongoDB fongoDB;

  private final FongoDBCollection fongoDBCollection;

  private final String map;

  private final String reduce;

  // TODO
  private final String finalize;

  private final DBObject out;

  private final DBObject query;

  private final DBObject sort;

  private final int limit;

  // http://docs.mongodb.org/manual/reference/method/db.collection.mapReduce/
  private enum Outmode {
    REPLACE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        coll.insert(results);
      }
    },
    MERGE {
      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        // Upsert == insert the result if not exist.
        for (DBObject result : results) {
          coll.update(new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY)), result, true,
              false);
        }
      }
    },
    REDUCE {
      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        List<DBObject> reduced = mr.reduceOutputStage(coll, results);
        for (DBObject result : reduced) {
          coll.update(new BasicDBObject(FongoDBCollection.ID_KEY, result.get(FongoDBCollection.ID_KEY)), result, true,
              false);
        }
      }
    },
    INLINE {
      @Override
      public void initCollection(DBCollection coll) {
        // Must replace all.
        coll.remove(new BasicDBObject());
      }

      @Override
      public void newResults(MapReduce mr, DBCollection coll, List<DBObject> results) {
        coll.insert(results);
      }

      @Override
      public String collectionName(DBObject object) {
        // Random uuid for extract result after.
        return UUID.randomUUID().toString();
      }

      // Return a list of all results.
      @Override
      public DBObject createResult(DBCollection coll) {
        BasicDBList list = new BasicDBList();
        list.addAll(coll.find().toArray());
        return list;
      }
    };

    public static Outmode valueFor(DBObject object) {
      for (Outmode outmode : values()) {
        if (object.containsField(outmode.name().toLowerCase())) {
          return outmode;
        }
      }
      return null;
    }

    public String collectionName(DBObject object) {
      return (String) object.get(name().toLowerCase());
    }

    public void initCollection(DBCollection coll) {
      // Do nothing.
    }

    public abstract void newResults(MapReduce marReduce, DBCollection coll, List<DBObject> results);

    public DBObject createResult(DBCollection coll) {
      DBObject result = new BasicDBObject("collection", coll.getName()).append("db", coll.getDB().getName());
      return result;
    }
  }

  public MapReduce(Fongo fongo, FongoDBCollection coll, String map, String reduce, String finalize, DBObject out, DBObject query, DBObject sort, Number limit) {
    this.fongo = fongo;
    if (out.containsField("db")) {
      this.fongoDB = (FongoDB) fongo.getDB((String) out.get("db"));
    } else {
      this.fongoDB = (FongoDB) coll.getDB();
    }
    this.fongoDBCollection = coll;
    this.map = map;
    this.reduce = reduce;
    this.finalize = finalize;
    this.out = out;
    this.query = query;
    this.sort = sort;
    this.limit = limit == null ? 0 : limit.intValue();
  }

  /**
   * @return null if error.
   */
  public DBObject computeResult() {
    // Replace, merge or reduce ?
    Outmode outmode = Outmode.valueFor(out);
    DBCollection coll = fongoDB.createCollection(outmode.collectionName(out), null);
    // Mode replace.
    outmode.initCollection(coll);
    outmode.newResults(this, coll, runInContext());
    DBObject result = outmode.createResult(coll);
    LOG.debug("computeResult() : {}", result);
    return result;
  }

  private List<DBObject> runInContext() {
    // TODO use Compilable ? http://www.jmdoudoux.fr/java/dej/chap-scripting.htm
    Context cx = Context.enter();
    try {
      Scriptable scope = cx.initStandardObjects();
      List<DBObject> objects = this.fongoDBCollection.find(query).sort(sort).limit(limit).toArray();
      List<String> javascriptFunctions = constructJavascriptFunction(objects);
      for (String jsFunction : javascriptFunctions) {
        try {
          cx.evaluateString(scope, jsFunction, "<map-reduce>", 0, null);
        } catch (RhinoException e) {
          LOG.error("Exception running script {}", jsFunction, e);
          fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
        }
      }

      // Get the result into an object.
      NativeArray outs = (NativeArray) scope.get("$$$fongoOuts$$$", scope);
      List<DBObject> dbOuts = new ArrayList<DBObject>();
      for (int i = 0; i < outs.getLength(); i++) {
        NativeObject out = (NativeObject) outs.get(i, outs);
        dbOuts.add(getObject(out));
      }
      return dbOuts;
    } finally {
      cx.exit();
    }
  }

  private List<DBObject> reduceOutputStage(DBCollection coll, List<DBObject> mapReduceOutput) {
    Context cx = Context.enter();
    try {
      Scriptable scope = cx.initStandardObjects();
      List<String> jsFunctions = constructReduceOutputStageJavascriptFunction(coll, mapReduceOutput);
      for (String jsFunction : jsFunctions) {
        try {
          cx.evaluateString(scope, jsFunction, "<reduce output stage>", 0, null);
        } catch (RhinoException e) {
          LOG.error("Exception running script {}", jsFunction, e);
          fongoDB.notOkErrorResult(16722, "JavaScript execution failed: " + e.getMessage()).throwOnError();
        }
      }

      // Get the result into an object.
      NativeArray outs = (NativeArray) scope.get("$$$fongoOuts$$$", scope);
      List<DBObject> dbOuts = new ArrayList<DBObject>();
      for (int i = 0; i < outs.getLength(); i++) {
        NativeObject out = (NativeObject) outs.get(i, outs);
        dbOuts.add(getObject(out));
      }

      LOG.debug("reduceOutputStage() : {}", dbOuts);
      return dbOuts;
    } finally {
      cx.exit();
    }
  }


  DBObject getObject(ScriptableObject no) {
    if (no instanceof NativeArray) {
        BasicDBList ret = new BasicDBList();
        NativeArray noArray = (NativeArray) no;
        for (int i = 0; i < noArray.getLength(); i++) {
            Object value = noArray.get(i, noArray);
            if (value instanceof NativeObject || value instanceof NativeArray) {
                value = getObject((ScriptableObject) value);
            }
            ret.add(value);
        }
        return ret;
    }
    DBObject ret = new BasicDBObject();
    Object[] propIds = no.getIds();
    for (Object propId : propIds) {
      String key = Context.toString(propId);
      Object value = NativeObject.getProperty(no, key);
      if (value instanceof NativeObject || value instanceof NativeArray) {
          value = getObject((ScriptableObject) value);
      }
      ret.put(key, value);
    }
    return ret;
  }

  /**
   * Create the map/reduce/finalize function.
   */
  private List<String> constructJavascriptFunction(List<DBObject> objects) {
    List<String> result = new ArrayList<String>();
    StringBuilder sb = new StringBuilder(80000);
    // Add some function to javascript engine.
    addMongoFunctions(sb);

    // Create variables for exporting.
    sb.append("var $$$fongoEmits$$$ = new Object();\n");
    sb.append("function emit(param1, param2) {\n" +
        "var toSource = param1.toSource();\n" +
        "if(typeof $$$fongoEmits$$$[toSource] === 'undefined') {\n " +
        "$$$fongoEmits$$$[toSource] = new Array();\n" +
        "}\n" +
        "var val = {id: param1, value: param2};\n" +
        "$$$fongoEmits$$$[toSource][$$$fongoEmits$$$[toSource].length] = val;\n" +
        "};\n");
    // Prepare map function.
    sb.append("var fongoMapFunction = ").append(map).append(";\n");
    sb.append("var $$$fongoVars$$$ = new Object();\n");
    // For each object, execute in javascript the function.
    for (DBObject object : objects) {
      String json = JSON.serialize(object);
      sb.append("$$$fongoVars$$$ = ").append(json).append(";\n");
      sb.append("$$$fongoVars$$$['fongoExecute'] = fongoMapFunction;\n");
      sb.append("$$$fongoVars$$$.fongoExecute();\n");
      if (sb.length() > 65535) { // Rhino limit :-(
        result.add(sb.toString());
        sb.setLength(0);
      }
    }
    result.add(sb.toString());

    // Add Reduce Function
    sb.setLength(0);
    sb.append("var reduce = ").append(reduce).append("\n");
    sb.append("var $$$fongoOuts$$$ = Array();\n" +
        "for(var i in $$$fongoEmits$$$) {\n" +
        "var elem = $$$fongoEmits$$$[i];\n" +
        "values = []; id = null; for (var ii in elem) { values.push(elem[ii].value); id = elem[ii].id;}\n" +
        "$$$fongoOuts$$$[$$$fongoOuts$$$.length] = { _id : id, value : reduce(id, values) };\n" +
        "}\n");
    result.add(sb.toString());

    return result;
  }

  /**
   * Create 'reduce' stage output function.
   */
  private List<String> constructReduceOutputStageJavascriptFunction(DBCollection coll, List<DBObject> objects) {
    List<String> result = new ArrayList<String>();
    StringBuilder sb = new StringBuilder(80000);

    addMongoFunctions(sb);

    sb.append("var reduce = ").append(reduce).append("\n");
    sb.append("var $$$fongoOuts$$$ = new Array();\n");
    for (DBObject object : objects) {
      String objectValue = JSON.serialize(object);

      DBObject existing = coll.findOne(new BasicDBObject().append(FongoDBCollection.ID_KEY,
          object.get(FongoDBCollection.ID_KEY)));
      if (existing == null) {
        sb.append("$$$fongoOuts$$$[$$$fongoOuts$$$.length] = ").append(objectValue).append(";\n");
      } else {
        String id = JSON.serialize(object.get(FongoDBCollection.ID_KEY));
        String existingValue = JSON.serialize(existing);
        sb.append("$$$fongoId$$$ = ").append(id).append(";\n");
        sb.append("$$$fongoValues$$$ = [ ").append(existingValue).append(", ").append(objectValue).append("];\n");
        sb.append("$$$fongoReduced$$$ = { _id : $$$fongoId$$$, value : reduce($$$fongoId$$$, $$$fongoValues$$$)};")
            .append(";\n");
        sb.append("$$$fongoOuts$$$[$$$fongoOuts$$$.length] = $$$fongoReduced$$$;\n");
      }
      if (sb.length() > 65535) { // Rhino limit :-(
        result.add(sb.toString());
        sb.setLength(0);
      }
    }
    result.add(sb.toString());
    return result;
  }

  private void addMongoFunctions(StringBuilder construct) {
    // Add some function to javascript engine.
    construct.append("Array.sum = function(array) {\n" +
        "    var a = 0;\n" +
        "    for (var i = 0; i < array.length; i++) {\n" +
        "        a = a + array[i];\n" +
        "    }\n" +
        "    return a;" +
        "};\n");
  }
}
