package org.folio.verticle.consumers.util;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    JournalParams getJournalParams(DataImportEventPayload eventPayload);
  }

  public enum JournalParamsEnum implements IJournalParams {

    DI_SRS_MARC_BIB_RECORD_UPDATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_SRS_MARC_BIB_RECORD_MODIFIED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.MODIFY,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_SRS_MARC_BIB_RECORD_NOT_MATCHED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.MARC_BIBLIOGRAPHIC,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_INSTANCE_CREATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_INSTANCE_UPDATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_INSTANCE_NOT_MATCHED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.INSTANCE,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_HOLDING_CREATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_HOLDING_UPDATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_HOLDING_NOT_MATCHED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.HOLDINGS,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_ITEM_CREATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_ITEM_UPDATED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_INVENTORY_ITEM_NOT_MATCHED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        return new JournalParams(JournalRecord.ActionType.NON_MATCH,
          JournalRecord.EntityType.ITEM,
          JournalRecord.ActionStatus.COMPLETED);
      }
    },
    DI_COMPLETED {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        String lastEventType = eventPayload.getEventsChain().stream().reduce((first, second) -> second).get();
        return JournalParamsEnum.getValue(lastEventType).getJournalParams(eventPayload);
      }
    },
    DI_ERROR {
      @Override
      public JournalParams getJournalParams(DataImportEventPayload eventPayload) {
        String lastEventType = eventPayload.getEventsChain().stream().reduce((first, second) -> second).get();
        JournalParams journalParams = JournalParamsEnum.getValue(lastEventType).getJournalParams(eventPayload);
        return new JournalParams(journalParams.journalActionType,
          journalParams.journalEntityType,
          JournalRecord.ActionStatus.ERROR);
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
