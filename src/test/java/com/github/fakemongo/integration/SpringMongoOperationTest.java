package com.github.fakemongo.integration;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.index.IndexInfo;

/**
 * User: william
 * Date: 15/03/14
 */
public class SpringMongoOperationTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  private MongoOperations mongoOperations;

  @Before
  public void before() throws Exception {
    Mongo mongo = fongoRule.getMongo();
    //Mongo mongo = new MongoClient();
    mongoOperations = new MongoTemplate(new SimpleMongoDbFactory(mongo, UUID.randomUUID().toString()));
  }

  @Test
  public void insertAndIndexesTest() {
    Item item = new Item(UUID.randomUUID(), "name", new Date());
    mongoOperations.insert(item);

    DBCollection collection = mongoOperations.getCollection(Item.COLLECTION_NAME);
    assertEquals(1, collection.count());

    IndexOperations indexOperations = mongoOperations.indexOps(Item.COLLECTION_NAME);
    System.out.println(indexOperations.getIndexInfo());
    boolean indexedId = false;
    boolean indexedName = false;
    for (IndexInfo indexInfo : indexOperations.getIndexInfo()) {
      if (indexInfo.isIndexForFields(Collections.singletonList("_id"))) {
        indexedId = true;
      }
      if (indexInfo.isIndexForFields(Collections.singletonList("name"))) {
        indexedName = true;
      }
    }
    Assertions.assertThat(indexedId).as("_id field is not indexedId").isTrue();
    Assertions.assertThat(indexedName).as("name field is not indexedId").isTrue();
  }
}
