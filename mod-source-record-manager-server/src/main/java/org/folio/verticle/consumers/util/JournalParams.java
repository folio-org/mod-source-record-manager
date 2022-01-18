package org.folio.verticle.consumers.util;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;

public class JournalParams {

  public JournalRecord.ActionType journalActionType;
  public JournalRecord.EntityType journalEntityType;
  public JournalRecord.ActionStatus journalActionStatus;

  public JournalParams(JournalRecord.ActionType journalActionType,
                       JournalRecord.EntityType journalEntityType,
                       JournalRecord.ActionStatus journalActionStatus) {
    this.journalActionType = journalActionType;
    this.journalEntityType = journalEntityType;
    this.journalActionStatus = journalActionStatus;
  }

  private interface IJournalParams {
    Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload);
  }

  public enum JournalParamsEnum implements IJournalParams {

    DI_SRS_MARC_BIB_RECORD_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_AUTHORITY_RECORD_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.MARC_AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_BIB_RECORD_MODIFIED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.MODIFY,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_BIB_RECORD_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.MATCH,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_AUTHORITY_RECORD_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.MATCH,
          JournalRecord.EntityType.MARC_AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_BIB_RECORD_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.MARC_AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_INSTANCE_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(CREATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(CREATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_INSTANCE_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_INSTANCE_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_HOLDING_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(CREATE,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_HOLDING_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_HOLDING_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_AUTHORITY_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(CREATE,
          JournalRecord.EntityType.AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_AUTHORITY_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_AUTHORITY_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_ITEM_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(CREATE,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_ITEM_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_INVENTORY_ITEM_NOT_MATCHED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_AUTHORITY_RECORD_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.MARC_AUTHORITY,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_LOG_SRS_MARC_BIB_RECORD_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_LOG_SRS_MARC_BIB_RECORD_UPDATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_SRS_MARC_HOLDING_RECORD_CREATED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        return Optional.of(new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.MARC_HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED));
      }
    },
    DI_COMPLETED {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        String lastEventType = eventPayload.getEventsChain().stream().reduce((first, second) -> second).get();
        JournalParamsEnum journalParamsEnumItem = NAMES.get(lastEventType);
        if (journalParamsEnumItem != null) {
          return journalParamsEnumItem.getJournalParams(eventPayload);
        }
        return Optional.empty();
      }
    },
    DI_ERROR {
      @Override
      public Optional<JournalParams> getJournalParams(DataImportEventPayload eventPayload) {
        if (CollectionUtils.isEmpty(eventPayload.getEventsChain())) {
          JournalRecord.EntityType sourceRecordType;
          if (eventPayload.getContext().containsKey(EDIFACT_INVOICE.value())) {
            sourceRecordType = JournalRecord.EntityType.EDIFACT;
          } else if (eventPayload.getContext().containsKey(JournalRecord.EntityType.MARC_HOLDINGS.value())) {
            sourceRecordType = JournalRecord.EntityType.MARC_HOLDINGS;
          } else if (eventPayload.getContext().containsKey(JournalRecord.EntityType.MARC_AUTHORITY.value())) {
            sourceRecordType = JournalRecord.EntityType.MARC_AUTHORITY;
          } else sourceRecordType = JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
          return Optional.of(new JournalParams(CREATE, sourceRecordType, ERROR));
        }

        String lastEventType = eventPayload.getEventsChain().stream().reduce((first, second) -> second).get();
        return JournalParamsEnum.getValue(lastEventType).getJournalParams(eventPayload)
          .map(journalParams -> new JournalParams(journalParams.journalActionType, journalParams.journalEntityType, ERROR));
      }
    };

    private static final Map<String, JournalParamsEnum> NAMES = Stream.of(values())
      .collect(Collectors.toMap(JournalParamsEnum::toString, Function.identity()));

    public static JournalParamsEnum getValue(final String name) {
      JournalParamsEnum value = NAMES.get(name);
      if (null == value) {
        throw new IllegalArgumentException(String.format("'%s' has no corresponding value. Accepted values: %s", name, Arrays.asList(values())));
      }
      return value;
    }

  }
}
