package org.folio.services;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.kafka.services.KafkaTopic;
import org.folio.services.kafka.SRMKafkaTopicService;
import org.folio.services.kafka.support.SRMKafkaTopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@RunWith(VertxUnitRunner.class)
public class KafkaAdminClientServiceTest {

  private final String STUB_TENANT = "foo-tenant";
  private KafkaAdminClient mockClient;
  private Vertx vertx;
  @Mock
  private SRMKafkaTopicService srmKafkaTopicService;

  @Before
  public void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
    srmKafkaTopicService = mock(SRMKafkaTopicService.class);
    KafkaTopic[] topicObjects = {
      new SRMKafkaTopic("DI_COMPLETE", 10),
      new SRMKafkaTopic("DI_ERROR", 10),
      new SRMKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_CREATED", 10),
      new SRMKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_CREATED", 10),
      new SRMKafkaTopic("DI_RAW_RECORDS_CHUNK_PARSED", 10),
      new SRMKafkaTopic("DI_MARC_FOR_UPDATE_RECEIVED", 10),
      new SRMKafkaTopic("DI_MARC_FOR_DELETE_RECEIVED", 10),
      new SRMKafkaTopic("DI_MARC_BIB_FOR_ORDER_CREATED", 10)
    };


    when(srmKafkaTopicService.createTopicObjects()).thenReturn(topicObjects);
  }

  @Test
  public void shouldCreateTopicIfAlreadyExist(TestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldFailIfExistExceptionIsPermanent(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(e -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldNotCreateTopicOnOther(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(cause -> {
          testContext.assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
        }
      ));
  }

  @Test
  public void shouldCreateTopicIfNotExist(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(allExpectedTopics.toArray()));
      }));
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().get(0).stream()
      .map(NewTopic::getName)
      .collect(Collectors.toList());
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(srmKafkaTopicService.createTopicObjects(), STUB_TENANT);
    }
  }

  private final Set<String> allExpectedTopics = Set.of(
    "test-env.foo-tenant.srm.DI_COMPLETE",
    "test-env.foo-tenant.srm.DI_ERROR",
    "test-env.foo-tenant.srm.DI_SRS_MARC_AUTHORITY_RECORD_CREATED",
    "test-env.foo-tenant.srm.DI_SRS_MARC_HOLDINGS_RECORD_CREATED",
    "test-env.foo-tenant.srm.DI_RAW_RECORDS_CHUNK_PARSED",
    "test-env.foo-tenant.srm.DI_MARC_FOR_UPDATE_RECEIVED",
    "test-env.foo-tenant.srm.DI_MARC_FOR_DELETE_RECEIVED",
    "test-env.foo-tenant.srm.DI_MARC_BIB_FOR_ORDER_CREATED"
  );
}
