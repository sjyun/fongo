package com.mongodb;

/**
 * Test result object which extends a {@link BasicDBObject}.
 * 
 * @author <a href="mailto:meder@adobe.com">Nils Meder</a>
 */
public class TestResultObject extends BasicDBObject {

  private static final long serialVersionUID = -7428775686006312017L;

  /**
   * Constructs a new {@link TestResultObject}.
   */
  public TestResultObject() {
    super();
  }

  /**
   * Returns the string of the {@code _id} field.
   * @return the string of the {@code _id} field.
   */
  public String getEntityId() {
    return this.getString("_id");
  }
}
