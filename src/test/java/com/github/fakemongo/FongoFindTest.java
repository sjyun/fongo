package com.github.fakemongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Rule;
import org.junit.Test;

public class FongoFindTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(!true);
  
  @Test
  public void testFindByNotExactType() {
    // Given
    DBCollection collection = fongoRule.newCollection();
    collection.insert(new BasicDBObject("_id", 1).append("field", 12L));
    
    // When
    DBObject result = collection.findOne(new BasicDBObject("field", 12));
    
    // Then
    assertEquals(new BasicDBObject("_id", 1).append("field", 12L), result);
  }


  /**
   * Checks that specified fields in find()'s projection from a list are actually returned (and are the only returned).
   */
  @Test
	public void testListFieldsProjection() {
    // Given
    DBCollection collection = fongoRule.newCollection();
    BasicDBList list = new BasicDBList();
    list.add(new BasicDBObject("a", "1").append("b", "2").append("c", "3"));
    list.add(new BasicDBObject("a", "4").append("b", "5").append("d", "6"));
    collection.insert(new BasicDBObject("_id", 1).append("lst", list));

    // When
    DBCursor cursor = collection.find(new BasicDBObject(), new BasicDBObject("lst.a", 1).append("lst.b", 1));

    // Then
    BasicDBList lst = (BasicDBList) cursor.next().get("lst");
    for (Object o : lst) {
      DBObject item = (DBObject) o;
      assertNotNull("'a' is expected from projection", item.get("a"));
      assertNotNull("'b' is expected from projection", item.get("b"));
      assertNull("'c' is not expected since it is not in projection", item.get("c"));
    }
  }
}
