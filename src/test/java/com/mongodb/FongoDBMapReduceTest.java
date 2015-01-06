package com.mongodb;

import com.github.fakemongo.Fongo;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 * @author bogdad@gmail.com
 */
public class FongoDBMapReduceTest {

  private Fongo fongo;
  private FongoDB db;
  private FongoDBCollection users;
  private FongoDBCollection typeHeights;

  @Before
  public void setUp() {
    fongo = new Fongo("test");
    db = (FongoDB) fongo.getDB("test");
    users = (FongoDBCollection) db.getCollection("users");
    typeHeights = (FongoDBCollection) db.getCollection("typeHeights");
  }

  @Test
  public void inline() {
    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");

    users.drop();
    users.insert(user1);
    users.insert(user2);
    users.insert(user3);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    MapReduceOutput result = users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.INLINE, new BasicDBObject());

    Iterable<DBObject> actual = result.results();
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150"))
    );
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200"))
    );
  }

  @Test
  public void replace() {
    BasicDBObject existingCat = new BasicDBObject()
        .append("_id", "cat")
        .append("value", new BasicDBObject().append("sum", "YY"));
    BasicDBObject existingNeutral = new BasicDBObject()
        .append("_id", "neutral")
        .append("value", new BasicDBObject().append("sum", "XX"));

    typeHeights.drop();
    typeHeights.insert(existingNeutral);
    typeHeights.insert(existingCat);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");

    users.drop();
    users.insert(user1);
    users.insert(user2);
    users.insert(user3);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.REPLACE, new BasicDBObject());

    Iterable<DBObject> actual = typeHeights.find();
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150"))
    );
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200"))
    );
    Assertions.assertThat(actual).doesNotContain(existingCat);
    Assertions.assertThat(actual).doesNotContain(existingNeutral);
  }

  @Test
  public void merge() {
    BasicDBObject existingCat = new BasicDBObject()
        .append("_id", "cat")
        .append("value", new BasicDBObject().append("sum", "YY"));
    BasicDBObject existingNeutral = new BasicDBObject()
        .append("_id", "neutral")
        .append("value", new BasicDBObject().append("sum", "XX"));

    typeHeights.drop();
    typeHeights.insert(existingNeutral);
    typeHeights.insert(existingCat);

    BasicDBObject user1 = new BasicDBObject().append("_id", "idUser1")
        .append("type", "neutral").append("height", "100");
    BasicDBObject user2 = new BasicDBObject().append("_id", "idUser2")
        .append("type", "neutral").append("height", "150");
    BasicDBObject user3 = new BasicDBObject().append("_id", "idUser3")
        .append("type", "human").append("height", "200");

    users.drop();
    users.insert(user1);
    users.insert(user2);
    users.insert(user3);

    String map = "function () {" +
        "emit(this.type, this);" +
        "};";
    String reduce = "function (key, values) {" +
        "  var sum = '';" +
        "  for (var i in values) {" +
        "    sum += values[i].height;" +
        "  }" +
        "  return {sum : sum};" +
        "}";

    users.mapReduce(map, reduce, typeHeights.getName(),
        MapReduceCommand.OutputType.MERGE, new BasicDBObject());

    Iterable<DBObject> actual = typeHeights.find();
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "neutral")
            .append("value", new BasicDBObject().append("sum", "100150"))
    );
    Assertions.assertThat(actual).contains(new BasicDBObject()
            .append("_id", "human")
            .append("value", new BasicDBObject().append("sum", "200"))
    );
    Assertions.assertThat(actual).contains(existingCat);
    Assertions.assertThat(actual).doesNotContain(existingNeutral);
  }
}
