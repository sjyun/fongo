package com.github.fakemongo.integration;

import java.util.Date;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * User: william
 * Date: 15/03/14
 */
@Document(collection = Item.COLLECTION_NAME)
public class Item {

  public static final String COLLECTION_NAME = "items";

  @Id
  private final UUID id;

  @Indexed
  @Field
  private final String name;

  @Field
  private final Date creationDateTime;

  public Item(UUID id, String name, Date creationDateTime) {
    this.id = id;
    this.name = name;
    this.creationDateTime = creationDateTime;
  }

  public UUID getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Date getCreationDateTime() {
    return creationDateTime;
  }

}