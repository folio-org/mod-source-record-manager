package org.folio.dao.util;

import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.cql.CQLWrapper;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

public class DaoUtil {

  private DaoUtil() {
  }

  /**
   * Build CQL from request URL query
   *
   * @param query - query from URL
   * @param limit - limit of results for pagination
   * @return - CQL wrapper for building postgres request to database
   * @throws org.z3950.zing.cql.cql2pgjson.FieldException field exception
   */
  public static CQLWrapper getCQLWrapper(String tableName, String query, int limit, int offset)
    throws org.z3950.zing.cql.cql2pgjson.FieldException {
    return getCQLWrapper(tableName, query)
      .setLimit(new Limit(limit))
      .setOffset(new Offset(offset));
  }

  /**
   * Build CQL from request URL query
   *
   * @param query - query from URL
   * @return - CQL wrapper for building postgres request to database
   * @throws org.z3950.zing.cql.cql2pgjson.FieldException field exception
   */
  public static CQLWrapper getCQLWrapper(String tableName, String query)
    throws org.z3950.zing.cql.cql2pgjson.FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(tableName + ".jsonb");
    return new CQLWrapper(cql2pgJson, query);
  }

  /**
   * Builds criteria by which db result is filtered
   *
   * @param jsonbField - json key name
   * @param value - value corresponding to the key
   * @return - Criteria object
   */
  public static Criteria constructCriteria(String jsonbField, String value) {
    Criteria criteria = new Criteria();
    criteria.addField(jsonbField);
    criteria.setOperation("=");
    criteria.setValue(value);
    return criteria;
  }
}
