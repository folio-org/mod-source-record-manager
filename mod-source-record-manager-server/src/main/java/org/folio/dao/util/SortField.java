package org.folio.dao.util;

public class SortField {

  private String field;
  private String order;

  public SortField(String field, String order) {
    this.field = field;
    this.order = order;
  }

  public String getField() {
    return field;
  }

  public String getOrder() {
    return order;
  }

  @Override
  public String toString() {
    return String.format("%s %s", field, order);
  }
}
