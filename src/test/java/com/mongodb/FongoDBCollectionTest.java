package com.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.github.fakemongo.Fongo;

public class FongoDBCollectionTest {

  private static final String ARBITRARY_ID = UUID.randomUUID().toString();
  
  private FongoDBCollection collection;

  @Before
  public void setUp() {
    collection = (FongoDBCollection) new Fongo("test").getDB("test").getCollection("test");
  }

  @Test
  public void applyProjectionsInclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 1));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("b", "b");

    assertEquals("applied", expected, actual);
  }
	
  @Test
  public void applyElemMatchProjectionsInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("a","a"));
    dbl.add(new BasicDBObject("b","b"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj, 
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch", new BasicDBObject("b", "b"))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("b","b"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }
	
  @Test
  public void applyElemMatchProjectionsMultiFieldInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("a","a").append("b", "b"));
    dbl.add(new BasicDBObject("c","c").append("d", "d"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj, 
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch", new BasicDBObject("d", "d"))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("c","c").append("d", "d"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }
	
  @Test
  public void applyElemMatchProjectionsMultiFieldWithIdInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("c","a").append("_id", "531ef0060bd4d252a197bdaf"));
    dbl.add(new BasicDBObject("c","c").append("_id", "531ef0060bd4d252a197bda7"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj, 
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch", 
                new BasicDBObject("_id", new ObjectId("531ef0060bd4d252a197bda7")))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("c","c").append("_id", "531ef0060bd4d252a197bda7"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }

  /** Tests multiprojections that are nested with the same prefix: a.b.c and a.b.d */
  @Test
  public void applyNestedProjectionsInclusionsOnly() {
      final DBObject obj = new BasicDBObjectBuilder()
          .add("_id", ARBITRARY_ID)
          .add("foo", 123)
          .push("a")
              .push("b")
                  .append("c", 50)
                  .append("d", 1000)
                  .append("bar", 1000)
      .get();

    final DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("a.b.c", 1)
                                                                                 .append("a.b.d", 1));
    final DBObject expected =  new BasicDBObjectBuilder()
                        .add("_id", ARBITRARY_ID)
                        .push("a")
                            .push("b")
                                .append("c", 50)
                                .append("d", 1000)
                           .get();

    assertEquals("applied", expected, actual);
  }


  @Test
  public void applyProjectionsInclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 1));
    BasicDBObject expected = new BasicDBObject().append("b", "b");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void applyProjectionsInclusionsAndExclusionsMixedThrowsException() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    collection.applyProjections(obj, new BasicDBObject().append("a", 1).append("b", 0));
  }


  @Test
  public void applyNestedArrayFieldProjection() {
    BasicDBObject obj = new BasicDBObject("_id", 1).append("name","foo")
      .append("seq", Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject("c", "b")));
    collection.insert(obj);

    List<DBObject> results = collection.find(new BasicDBObject("_id", 1),
        new BasicDBObject("_id", -1).append("seq.a", 1)).toArray();

    BasicDBObject expectedResult = new BasicDBObject("seq",
      Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject()));

    assertEquals("should have projected result", Arrays.asList(expectedResult), results);
  }

  @Test
  public void applyNestedFieldProjection() {

    collection.insert(new BasicDBObject("_id", 1)
      .append("a",new BasicDBObject("b", new BasicDBObject("c", 1))));

    collection.insert(new BasicDBObject("_id", 2)
      .append("a",new BasicDBObject("b", 1)));

    collection.insert(new BasicDBObject("_id", 3));

    List<DBObject> results = collection.find(new BasicDBObject(),
        new BasicDBObject("_id", -1).append("a.b.c", 1)).toArray();

    assertEquals("should have projected result", Arrays.asList(
      new BasicDBObject("a",new BasicDBObject("b", new BasicDBObject("c", 1))),
      new BasicDBObject("a",new BasicDBObject()),
      new BasicDBObject()
    ), results);
  }

  @Test
  public void findByListInQuery(){
    BasicDBObject existing = new BasicDBObject().append("_id", 1).append("aList", asDbList("a", "b", "c"));
    collection.insert(existing);
    DBObject result = collection.findOne(existing);
    assertEquals("should have projected result", existing, result);
  }

  BasicDBList asDbList(Object ... objects) {
     BasicDBList list = new BasicDBList();
     list.addAll(Arrays.asList(objects));
     return list;
  }

  /** Tests multiprojections that are nested with the same prefix: a.b.c and a.b.d */
  @Test
  public void applyProjectionsWithBooleanValues() {
     final DBObject obj = new BasicDBObjectBuilder()
          .add("_id", ARBITRARY_ID)
          .add("foo", "oof")
          .add("bar", "rab")
          .add("gone", "fishing")
      .get();

    final DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("foo", true)
                                                                                 .append("bar", 1));
    final DBObject expected =  new BasicDBObjectBuilder()
                        .add("_id", ARBITRARY_ID)
                        .add("foo", "oof")
                        .add("bar", "rab")
                        .get();

    assertEquals("applied", expected, actual);
  }


  @Test
  public void filterListDoentModifyEntry() {
    DBObject object = new BasicDBObject() {
        @Override
        public Object put(String key, Object val) {
            throw new IllegalStateException();
        }

        @Override
        public void putAll(Map m) {
            throw new IllegalStateException();
        }

        @Override
        public void putAll(BSONObject o) {
            throw new IllegalStateException();
        }

        @Override
        public BasicDBObject append(String key, Object val) {
            throw new IllegalStateException();
        }
    };

    DBObject r = collection.filterLists(object);
    assertTrue(r != object);
  }

  @Test
  public void setResultObjectClassForFindAndModify() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);
    
    final TestResultObject result = (TestResultObject) collection.findAndModify(object, new BasicDBObject());
    final String id = result.getEntityId();
    assertThat(id).isEqualTo(ARBITRARY_ID);
  }
  
  @Test
  public void setResultObjectClassForFindOne() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);
    
    final TestResultObject result = (TestResultObject) collection.findOne(object, new BasicDBObject());
    final String id = result.getEntityId();
    assertThat(id).isEqualTo(ARBITRARY_ID);
  }
  
  @Test
  public void setResultObjectClassFind() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);
    
    final DBCursor cursorWithKeys = collection.find(object, new BasicDBObject());
    final TestResultObject resultWithKeys = (TestResultObject) cursorWithKeys.next();
    assertThat(resultWithKeys.getEntityId()).isEqualTo(ARBITRARY_ID);

    final DBCursor cursorWithQuery = collection.find(object);
    final TestResultObject resultWithQuery = (TestResultObject) cursorWithQuery.next();
    assertThat(resultWithQuery.getEntityId()).isEqualTo(ARBITRARY_ID);

    final DBCursor cursor = collection.find();
    final TestResultObject result = (TestResultObject) cursor.next();
    assertThat(result.getEntityId()).isEqualTo(ARBITRARY_ID);
  }

  @Test
  public void textSearch() {
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("textField", "tomorrow, and tomorrow, and tomorrow, creeps in this petty pace");
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("textField", "eee, abc def");
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("textField", "bbb, ccc");
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id4")
            .append("textField", "aaa, bbb");
    BasicDBObject obj5 = new BasicDBObject().append("_id", "_id5")
            .append("textField", "bbb, fff");
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    collection.insert(obj4);
    collection.insert(obj5);
    collection.createIndex(new BasicDBObject("textField", "text"));
    DBObject actual = collection.text("aaa bbb -ccc -ddd -яяя \"abc def\" \"def bca\"", 0, new BasicDBObject());
    
    BasicDBList resultsExpected = new BasicDBList();
      resultsExpected.add(new BasicDBObject("score", 1.5)
              .append("obj", new BasicDBObject("_id", "_id2").append("textField", "eee, abc def")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id4").append("textField", "aaa, bbb")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id5").append("textField", "bbb, fff")));
    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", resultsExpected);            
    expected.put("stats", 
            new BasicDBObject("nscannedObjects", 6L)
            .append("nscanned", 5L)
            .append("n", 3L)
            .append("timeMicros", 1));
    expected.put("ok", 1);
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void textSearchExactMatch() {
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("textField", "tomorrow, and tomorrow, and tomorrow, creeps in this petty pace");
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("textField", "eee, abc def");
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("textField", "bbb, ccc");
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id4")
            .append("textField", "aaa, bbb");
    BasicDBObject obj5 = new BasicDBObject().append("_id", "_id5")
            .append("textField", "bbb, fff");
    BasicDBObject obj6 = new BasicDBObject().append("_id", "_id6")
            .append("textField", "aaa aaa eee, abc def");
    BasicDBObject obj7 = new BasicDBObject().append("_id", "_id7")
            .append("textField", "aaaaaaa");
    BasicDBObject obj8 = new BasicDBObject().append("_id", "_id8")
            .append("textField", "aaaaaaaa");
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    collection.insert(obj4);
    collection.insert(obj5);
    collection.insert(obj6);
    collection.insert(obj8);
    collection.createIndex(new BasicDBObject("textField", "text"));
    DBObject actual = collection.text("aaa", 0, new BasicDBObject("textField", 1));
    
    BasicDBList resultsExpected = new BasicDBList();
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id4").append("textField", "aaa, bbb")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id6").append("textField", "aaa aaa eee, abc def")));
    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", resultsExpected);            
    expected.put("stats", 
            new BasicDBObject("nscannedObjects", 2L)
            .append("nscanned", 2L)
            .append("n", 2L)
            .append("timeMicros", 1));
    expected.put("ok", 1);
    Assertions.assertThat(actual).isEqualTo(expected);
  }
}
