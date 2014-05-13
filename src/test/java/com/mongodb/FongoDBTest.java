package com.mongodb;

import com.github.fakemongo.Fongo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Anton Bobukh <abobukh@yandex-team.ru>
 */
public class FongoDBTest {

  private final int options = 0;
  private final ReadPreference preference = ReadPreference.nearest();

  private FongoDB db;

  @Before
  public void setUp() {
    db = (FongoDB) new Fongo("test").getDB("test");
  }

  @Test
  public void commandGetLastErrorAliases() {
    BasicDBObject command;

    command = new BasicDBObject("getlasterror", 1);
    Assert.assertTrue(db.command(command, options, preference).containsField("ok"));

    command = new BasicDBObject("getLastError", 1);
    Assert.assertTrue(db.command(command, options, preference).containsField("ok"));
  }

  @Test
  public void commandFindAndModifyAliases() {
    BasicDBObject command;

    command = new BasicDBObject("findandmodify", "test");
    Assert.assertTrue(db.command(command, options, preference).containsField("value"));

    command = new BasicDBObject("findAndModify", "test");
    Assert.assertTrue(db.command(command, options, preference).containsField("value"));
  }

  @Test
  public void commandBuildInfoAliases() {
    BasicDBObject command;

    command = new BasicDBObject("buildinfo", 1);
    Assert.assertTrue(db.command(command, options, preference).containsField("version"));

    command = new BasicDBObject("buildInfo", 1);
    Assert.assertTrue(db.command(command, options, preference).containsField("version"));
  }

  @Test
  public void commandMapReduceAliases() {
    BasicDBObject command;

    command = new BasicDBObject("mapreduce", "test").append("out", new BasicDBObject("inline", 1));
    Assert.assertTrue(db.command(command, options, preference).containsField("results"));

    command = new BasicDBObject("mapReduce", "test").append("out", new BasicDBObject("inline", 1));
    Assert.assertTrue(db.command(command, options, preference).containsField("results"));
  }

}
