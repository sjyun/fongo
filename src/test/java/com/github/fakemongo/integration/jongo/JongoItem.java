package com.github.fakemongo.integration.jongo;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.jongo.marshall.jackson.oid.Id;

public class JongoItem {

  @Id
  private JongoItemId id;

  private String field;

  public JongoItem() {
  }

  public JongoItemId getId() {
    return this.id;
  }

  public String getField() {
    return this.field;
  }

  public void setId(JongoItemId id) {
    this.id = id;
  }

  public void setField(String field) {
    this.field = field;
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof JongoItem)) return false;
    final JongoItem other = (JongoItem) o;
    if (!other.canEqual((Object) this)) return false;
    final Object this$id = this.id;
    final Object other$id = other.id;
    if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
    final Object this$field = this.field;
    final Object other$field = other.field;
    if (this$field == null ? other$field != null : !this$field.equals(other$field)) return false;
    return true;
  }

  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final Object $id = this.id;
    result = result * PRIME + ($id == null ? 0 : $id.hashCode());
    final Object $field = this.field;
    result = result * PRIME + ($field == null ? 0 : $field.hashCode());
    return result;
  }

  protected boolean canEqual(Object other) {
    return other instanceof JongoItem;
  }

  public String toString() {
    return "com.github.fakemongo.integration.jongo.JongoItem(id=" + this.id + ", field=" + this.field + ")";
  }

  public static class JongoItemId {
    private String name, id;

    @java.beans.ConstructorProperties({"name", "id"})
    public JongoItemId(@JsonProperty("name") String name, @JsonProperty("id") String id) {
      this.name = name;
      this.id = id;
    }

    public String getName() {
      return this.name;
    }

    public String getId() {
      return this.id;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setId(String id) {
      this.id = id;
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof JongoItemId)) return false;
      final JongoItemId other = (JongoItemId) o;
      if (!other.canEqual((Object) this)) return false;
      final Object this$name = this.name;
      final Object other$name = other.name;
      if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
      final Object this$id = this.id;
      final Object other$id = other.id;
      if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
      return true;
    }

    public int hashCode() {
      final int PRIME = 59;
      int result = 1;
      final Object $name = this.name;
      result = result * PRIME + ($name == null ? 0 : $name.hashCode());
      final Object $id = this.id;
      result = result * PRIME + ($id == null ? 0 : $id.hashCode());
      return result;
    }

    protected boolean canEqual(Object other) {
      return other instanceof JongoItemId;
    }

    public String toString() {
      return "com.github.fakemongo.integration.jongo.JongoItem.JongoItemId(name=" + this.name + ", id=" + this.id + ")";
    }
  }
}
