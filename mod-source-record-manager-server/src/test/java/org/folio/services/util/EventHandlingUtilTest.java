package org.folio.services.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;

import org.folio.services.exceptions.RecordsPublishingException;
import org.junit.jupiter.api.Test;

class EventHandlingUtilTest {

  @Test
  void constructModuleName() {
    assertThat(EventHandlingUtil.constructModuleName(), matchesRegex("mod-source-record-manager-\\d.+"));
  }

  @Test
  void handleKafkaPublishingErrors() {
    var runtimeException = new RuntimeException("foo");
    var future = EventHandlingUtil.handleKafkaPublishingErrors("{}", null, null, runtimeException);
    assertThat(future.cause(), is(runtimeException));
  }

  @Test
  void handleKafkaPublishingErrorsRecords() {
    var payload = """
                  { "records": [
                      { "id": "4c64addc-1066-40ca-b287-1593ae41e33f" }
                    ]
                  }
                  """;
    var runtimeException = new RuntimeException("bar");
    var future = EventHandlingUtil.handleKafkaPublishingErrors(payload, null, null, runtimeException);
    RecordsPublishingException e = (RecordsPublishingException) future.cause();
    assertThat(e.getMessage(), is("bar"));
    assertThat(e.getFailedRecords().get(0).getId(), is("4c64addc-1066-40ca-b287-1593ae41e33f"));
  }

}
