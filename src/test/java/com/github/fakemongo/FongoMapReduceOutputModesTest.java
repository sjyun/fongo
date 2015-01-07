package com.github.fakemongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.FongoDB;
import com.mongodb.FongoDBCollection;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Vladimir Shakhov <bogdad@gmail.com>
 */
public class FongoMapReduceOutputModesTest {

  private Fongo fongo;
  private FongoDB db;

  private FongoDBCollection users;
  private FongoDBCollection typeHeights;
  private FongoDBCollection userLogins;
  private FongoDBCollection joinUsersLogins;

  @Before
  public void setUp() {
    fongo = new Fongo("test");
    db = (FongoDB) fongo.getDB("test");
    users = (FongoDBCollection) db.getCollection("users");
    userLogins = (FongoDBCollection) db.getCollection("userLogins");
    typeHeights = (FongoDBCollection) db.getCollection("typeHeights");
    joinUsersLogins = (FongoDBCollection) db.getCollection("joinUsersLogins");
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

  @Test
  public void reduceForJoinDataAllreadyThere() {

    joinUsersLogins.insert(new BasicDBObject()
        .append("_id", "idUser1")
        .append("somekey", "somevalue"));

    BasicDBObject user1Login = new BasicDBObject()
        .append("_id", "idUser1")
        .append("login", "bloble");
    BasicDBObject user2Login = new BasicDBObject()
        .append("_id", "idUser2")
        .append("login", "wwww");
    BasicDBObject user3Login = new BasicDBObject()
        .append("_id", "idUser3")
        .append("login", "wordpress");

    userLogins.drop();
    userLogins.insert(user1Login);
    userLogins.insert(user2Login);
    userLogins.insert(user3Login);

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

    String mapUsers = "function () {" +
        "emit(this._id, {value : this});" +
        "};";
    String mapUserLogins = "function () {" +
        "emit(this._id, {value : this});" +
        "};";
    String reduce = "function (id, values) {" +
        "function ifnull(r, v, key) {\n" +
        "  if (v[key] != undefined) r[key] = v[key];\n" +
        "  return r;\n" +
        "  }\n" +
        "  function ifnulls(r, v, keys) {\n" +
        "    for(var i in keys) r = ifnull(r, v, keys[i]);\n" +
        "    return r;\n" +
        "  }\n" +
        "  res = {};\n" +
        "  for (var i in values) {\n" +
        "    if (!('value' in values[i])) continue;" +
        "    res = ifnulls(res, values[i].value, ['_id', 'login', 'type', 'height']);\n" +
        "  }\n" +
        "  return res;\n" +
        "}";

    users.mapReduce(mapUsers, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    userLogins.mapReduce(mapUserLogins, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());

    Iterable<DBObject> actual = joinUsersLogins.find();

    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user1.get("_id"))
        .append("value", user1.append("login", user1Login.get("login"))));
    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user2.get("_id"))
        .append("value", user2.append("login", user2Login.get("login"))));
    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user3.get("_id"))
        .append("value", user3.append("login", user3Login.get("login"))));
  }

  @Test
  public void reduceForJoin() {

    BasicDBObject user1Login = new BasicDBObject()
        .append("_id", "idUser1")
        .append("login", "bloble");
    BasicDBObject user2Login = new BasicDBObject()
        .append("_id", "idUser2")
        .append("login", "wwww");
    BasicDBObject user3Login = new BasicDBObject()
        .append("_id", "idUser3")
        .append("login", "wordpress");

    userLogins.drop();
    userLogins.insert(user1Login);
    userLogins.insert(user2Login);
    userLogins.insert(user3Login);

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

    String mapUsers = "function () {" +
        "emit(this._id, {value : this});" +
        "};";
    String mapUserLogins = "function () {" +
        "emit(this._id, {value : this});" +
        "};";
    String reduce = "function (id, values) {" +
        "function ifnull(r, v, key) {\n" +
        "  if (v[key] != undefined) r[key] = v[key];\n" +
        "  return r;\n" +
        "  }\n" +
        "  function ifnulls(r, v, keys) {\n" +
        "    for(var i in keys) r = ifnull(r, v, keys[i]);\n" +
        "    return r;\n" +
        "  }\n" +
        "  res = {};\n" +
        "  for (var i in values) {\n" +
        "    res = ifnulls(res, values[i].value, ['_id', 'login', 'type', 'height']);\n" +
        "  }\n" +
        "  return res;\n" +
        "}";

    users.mapReduce(mapUsers, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());
    userLogins.mapReduce(mapUserLogins, reduce, joinUsersLogins.getName(),
        MapReduceCommand.OutputType.REDUCE, new BasicDBObject());

    Iterable<DBObject> actual = joinUsersLogins.find();

    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user1.get("_id"))
        .append("value", user1.append("login", user1Login.get("login"))));
    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user2.get("_id"))
        .append("value", user2.append("login", user2Login.get("login"))));
    Assertions.assertThat(actual).contains(new BasicDBObject()
        .append("_id", user3.get("_id"))
        .append("value", user3.append("login", user3Login.get("login"))));
  }
}
