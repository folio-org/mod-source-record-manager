package org.folio.services.util;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RecordConversionUtilTest {

  @Test
  public void shouldReturnMarcBib() {
    EntityType entityType = RecordConversionUtil.getEntityType(new Record().withRecordType(Record.RecordType.MARC_BIB));

    assertEquals(EntityType.MARC_BIBLIOGRAPHIC, entityType);
  }

  @Test
  public void shouldReturnMarcAuthority() {
    EntityType entityType = RecordConversionUtil.getEntityType(new Record().withRecordType(Record.RecordType.MARC_AUTHORITY));

    assertEquals(EntityType.MARC_AUTHORITY, entityType);
  }

  @Test
  public void shouldReturnMarcHolding() {
    EntityType entityType = RecordConversionUtil.getEntityType(new Record().withRecordType(Record.RecordType.MARC_HOLDING));

    assertEquals(EntityType.MARC_HOLDINGS, entityType);
  }

  @Test
  public void shouldReturnEdifactInvoice() {
    EntityType entityType = RecordConversionUtil.getEntityType(new Record().withRecordType(Record.RecordType.EDIFACT));

    assertEquals(EntityType.EDIFACT_INVOICE, entityType);
  }
}
