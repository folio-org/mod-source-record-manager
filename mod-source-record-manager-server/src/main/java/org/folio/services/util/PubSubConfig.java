package org.folio.services.util;

import static java.lang.String.join;

public class PubSubConfig {
  private static final String PUB_SUB_PREFIX = "pub-sub";
  private String tenant;
  private String eventType;
  private String groupId;
  private String topicName;

  public PubSubConfig(String env, String tenant, String eventType) {
    this.tenant = tenant;
    this.eventType = eventType;
    this.groupId = join(".", env, PUB_SUB_PREFIX, tenant, eventType);
    this.topicName = join(".", env, PUB_SUB_PREFIX, tenant, eventType);
  }

  public String getTenant() {
    return tenant;
  }

  public String getEventType() {
    return eventType;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getTopicName() {
    return topicName;
  }
}
