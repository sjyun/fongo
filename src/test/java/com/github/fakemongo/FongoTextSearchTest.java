/*
 * Copyright 2014 Alexander Arutuniants <alex.art@in2circle.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fakemongo;

import com.github.fakemongo.impl.text.TextSearch;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.List;
import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Alexander Arutuniants <alex.art@in2circle.com>
 */
public class FongoTextSearchTest {

  private DBCollection collection;
  private TextSearch ts;

  @Rule
  public FongoRule fongoRule = new FongoRule(!true);

  @Before
  public void setUp() {
    collection = fongoRule.newCollection();
    collection.insert((DBObject) JSON.parse("{ _id:1, textField: \"aaa bbb\", otherField: \"text1 aaa\" }"));
    collection.insert((DBObject) JSON.parse("{ _id:2, textField: \"ccc ddd\", otherField: \"text2 aaa\" }"));
    collection.insert((DBObject) JSON.parse("{ _id:3, textField: \"eee fff\", otherField: \"text3 aaa\" }"));
    collection.insert((DBObject) JSON.parse("{ _id:4, textField: \"aaa eee\", otherField: \"text4 aaa\" }"));

    collection.createIndex(new BasicDBObject("textField", "text"));

    ts = new TextSearch(collection);
  }

  @Test
  public void testFindByTextSearch_String() {
    String searchString = "aaa -eee -bbb";

    DBObject result = ts.findByTextSearch(searchString);

    Assertions.assertThat(((List) result.get("results"))).hasSize(0);

    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", new BasicDBList());
    expected.put("stats",
        new BasicDBObject("nscannedObjects", 5L)
            .append("nscanned", 2L)
            .append("n", 0L)
            .append("timeMicros", 1)
    );
    expected.put("ok", 1);
    Assertions.assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testFindByTextSearch_String_DBObject() {
    String searchString = "aaa -eee";
    DBObject project = new BasicDBObject("textField", 1);

    DBObject result = ts.findByTextSearch(searchString, project);

    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", JSON.parse("[ { "
        + "\"score\" : 0.75 , "
        + "\"obj\" : { \"_id\" : 1 , \"textField\" : \"aaa bbb\"}}]"));
    expected.put("stats",
        new BasicDBObject("nscannedObjects", 4L)
            .append("nscanned", 2L)
            .append("n", 1L)
            .append("timeMicros", 1)
    );
    expected.put("ok", 1);
    Assertions.assertThat(result).isEqualTo(expected);
    assertEquals("aaa bbb",
        ((DBObject) ((DBObject) ((List) result.get("results")).get(0)).get("obj")).get("textField"));

  }

  @Test
  public void testFindByTextSearch_3args() {
    String searchString = "aaa bbb ccc ddd eee";
    DBObject project = new BasicDBObject("textField", 1).append("otherField", 1);

    DBObject result = ts.findByTextSearch(searchString, project, 2);

    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", JSON.parse("[ "
        + "{ \"score\" : 1.5 , "
        + "\"obj\" : { \"_id\" : 1 , \"textField\" : \"aaa bbb\" , \"otherField\" : \"text1 aaa\"}} , "
        + "{ \"score\" : 1.5 , "
        + "\"obj\" : { \"_id\" : 2 , \"textField\" : \"ccc ddd\" , \"otherField\" : \"text2 aaa\"}}]"));
    expected.put("stats",
        new BasicDBObject("nscannedObjects", 6L)
            .append("nscanned", 6L)
            .append("n", 2L)
            .append("timeMicros", 1)
    );
    expected.put("ok", 1);
    Assertions.assertThat(result).isEqualTo(expected);
    assertEquals("ccc ddd",
        ((DBObject) ((DBObject) ((List) result.get("results")).get(1)).get("obj")).get("textField"));
  }
}
