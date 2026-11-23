package org.folio.dao.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import org.junit.Test;

public class DbUtilTest {

  @Test
  public void shouldReadEpochMillis_whenCreatedDateStoredAsNumber() {
    // when
    Date date = DbUtil.toDate(1_700_000_000_000L);

    // then
    assertThat(date).isEqualTo(new Date(1_700_000_000_000L));
  }

  @Test
  public void shouldReadIsoInstant_whenCreatedDateStoredAsString() {
    // when
    Date date = DbUtil.toDate("2023-11-14T22:13:20Z");

    // then
    assertThat(date).isEqualTo(new Date(1_700_000_000_000L));
  }
}
