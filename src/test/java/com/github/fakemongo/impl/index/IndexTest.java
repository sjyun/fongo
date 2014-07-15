package com.github.fakemongo.impl.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link Index}.
 * 
 * @author <a href="mailto:meder@adobe.com">Nils Meder</a>
 * 
 */
public class IndexTest {

  // ------------------------------------------------------------------------------------------------- Private Constants

  private static final String INDEX_NAME = UUID.randomUUID().toString();

  private static final boolean UNIQUE = true;

  private static final String ID_KEY = "_id";

  private static final String ID = UUID.randomUUID().toString();

  private static final DBObject DEFAULT_ID_KEY = new BasicDBObject(ID_KEY, 1);

  private static final String TOP_LEVEL_KEY = UUID.randomUUID().toString();

  private static final String SECOND_LEVEL_KEY = UUID.randomUUID().toString();

  private static final String SECOND_LEVEL_VALUE = UUID.randomUUID().toString();

  private static final String THIRD_LEVEL_KEY = UUID.randomUUID().toString();

  private static final String FOURTH_LEVEL_KEY = UUID.randomUUID().toString();

  private static final String FIFTH_LEVEL_KEY = UUID.randomUUID().toString();

  private static final String FIFTH_LEVEL_VALUE = UUID.randomUUID().toString();

  // ---------------------------------------------------------------------------------------------------- Public Methods

  /**
   * Test {@link Index#embedded(DBObject)} with an expended {@link DBObject}.
   */
  @Test
  public void testEmbeddedExpandedDBObject() {
    final DBObject sourceObject = new BasicDBObject();
    sourceObject.put(ID_KEY, ID);
    sourceObject.put(TOP_LEVEL_KEY, new BasicDBObject(SECOND_LEVEL_KEY, SECOND_LEVEL_VALUE));

    final Index iut = new Index(INDEX_NAME, DEFAULT_ID_KEY, UNIQUE);
    final DBObject resultObject = iut.embedded(sourceObject);

    assertThat(resultObject).isEqualTo(sourceObject);
    assertThat(resultObject.containsField(ID_KEY)).isTrue();
    assertThat(resultObject.containsField(TOP_LEVEL_KEY)).isTrue();

    final DBObject topLevelObject = (DBObject) resultObject.get(TOP_LEVEL_KEY);
    assertThat(topLevelObject.containsField(SECOND_LEVEL_KEY)).isTrue();

    final String secondLevelValue = (String) topLevelObject.get(SECOND_LEVEL_KEY);
    assertThat(secondLevelValue).isEqualTo(SECOND_LEVEL_VALUE);
  }

  /**
   * Test {@link Index#embedded(DBObject)} with an flattened {@link DBObject}.
   */
  @Test
  public void testEmbeddedFlattenedDBObject() {
    final DBObject sourceObject = new BasicDBObject();
    sourceObject.put(ID_KEY, ID);
    sourceObject.put(TOP_LEVEL_KEY + "." + SECOND_LEVEL_KEY, SECOND_LEVEL_VALUE);

    final Index iut = new Index(INDEX_NAME, DEFAULT_ID_KEY, UNIQUE);
    final DBObject resultObject = iut.embedded(sourceObject);

    assertThat(resultObject).isEqualTo(sourceObject);
    assertThat(resultObject.containsField(ID_KEY)).isTrue();
    assertThat(resultObject.containsField(TOP_LEVEL_KEY)).isTrue();

    final DBObject topLevelObject = (DBObject) resultObject.get(TOP_LEVEL_KEY);
    assertThat(topLevelObject.containsField(SECOND_LEVEL_KEY)).isTrue();

    final String secondLevelValue = (String) topLevelObject.get(SECOND_LEVEL_KEY);
    assertThat(secondLevelValue).isEqualTo(SECOND_LEVEL_VALUE);
  }

  /**
   * Test {@link Index#embedded(DBObject)} with an flattened {@link DBObject}
   * with five levels depth.
   */
  @Test
  public void testEmbeddedFlattenedDBObjectWithFiveLevelsDepth() {
    final DBObject sourceObject = new BasicDBObject();
    sourceObject.put(ID_KEY, ID);
    sourceObject.put(TOP_LEVEL_KEY + "." + SECOND_LEVEL_KEY + "." + THIRD_LEVEL_KEY + "." + FOURTH_LEVEL_KEY + "."
        + FIFTH_LEVEL_KEY, FIFTH_LEVEL_VALUE);

    final Index iut = new Index(INDEX_NAME, DEFAULT_ID_KEY, UNIQUE);
    final DBObject resultObject = iut.embedded(sourceObject);

    assertThat(resultObject).isEqualTo(sourceObject);
    assertThat(resultObject.containsField(ID_KEY)).isTrue();
    assertThat(resultObject.containsField(TOP_LEVEL_KEY)).isTrue();

    final DBObject topLevelObject = (DBObject) resultObject.get(TOP_LEVEL_KEY);
    assertThat(topLevelObject.containsField(SECOND_LEVEL_KEY)).isTrue();

    final DBObject secondLevelObject = (DBObject) topLevelObject.get(SECOND_LEVEL_KEY);
    assertThat(secondLevelObject.containsField(THIRD_LEVEL_KEY)).isTrue();

    final DBObject thirdLevelObject = (DBObject) secondLevelObject.get(THIRD_LEVEL_KEY);
    assertThat(thirdLevelObject.containsField(FOURTH_LEVEL_KEY)).isTrue();

    final DBObject fourthLevelObject = (DBObject) thirdLevelObject.get(FOURTH_LEVEL_KEY);
    assertThat(fourthLevelObject.containsField(FIFTH_LEVEL_KEY)).isTrue();

    final String fithLevelValue = (String) fourthLevelObject.get(FIFTH_LEVEL_KEY);
    assertThat(fithLevelValue).isEqualTo(FIFTH_LEVEL_VALUE);
  }

  /**
   * Test {@link Index#embedded(DBObject)} with an flattened {@link DBObject}
   * with two elements on the same level.
   */
  @Test
  public void testEmbeddedFlattenedDBObjectWithTwoElementOnTheSameLevel() {
    final DBObject sourceObject = new BasicDBObject();
    sourceObject.put(ID_KEY, ID);
    sourceObject.put(TOP_LEVEL_KEY + "." + SECOND_LEVEL_KEY, SECOND_LEVEL_VALUE);
    sourceObject.put(TOP_LEVEL_KEY + "." + FIFTH_LEVEL_KEY, FIFTH_LEVEL_VALUE);

    final Index iut = new Index(INDEX_NAME, DEFAULT_ID_KEY, UNIQUE);
    final DBObject resultObject = iut.embedded(sourceObject);

    assertThat(resultObject).isEqualTo(sourceObject);
    assertThat(resultObject.containsField(ID_KEY)).isTrue();
    assertThat(resultObject.containsField(TOP_LEVEL_KEY)).isTrue();

    final DBObject topLevelObject = (DBObject) resultObject.get(TOP_LEVEL_KEY);
    assertThat(topLevelObject.containsField(SECOND_LEVEL_KEY)).isTrue();
    assertThat(topLevelObject.containsField(FIFTH_LEVEL_KEY)).isTrue();

    final String secondLevelObject = (String) topLevelObject.get(SECOND_LEVEL_KEY);
    assertThat(secondLevelObject).isEqualTo(SECOND_LEVEL_VALUE);

    final String fithLevelValue = (String) topLevelObject.get(FIFTH_LEVEL_KEY);
    assertThat(fithLevelValue).isEqualTo(FIFTH_LEVEL_VALUE);
  }
}
