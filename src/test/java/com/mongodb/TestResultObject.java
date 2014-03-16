/*************************************************************************
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 *  Copyright 2014 Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by all applicable intellectual property
 * laws, including trade secret and or copyright laws.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
package com.mongodb;



/**
 * Test result object which extends a {@link BasicDBObject}.
 * 
 * @author <a href="mailto:meder@adobe.com">Nils Meder</a>
 * 
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
