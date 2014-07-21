package com.github.fakemongo.integration.jongo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jongo.marshall.jackson.oid.Id;

@Data
public class JongoItem {

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class JongoItemId {
    private String name, id;
  }

  @Id
  private JongoItemId id;

  private String field;
}
