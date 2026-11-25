package org.folio.services;

import static java.lang.Boolean.TRUE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.NON_MATCH;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.ACTION_FIELD;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.CREATE_ACTION;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.FOLIO_RECORD;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.ORDER_TYPE;

import io.vertx.core.json.jackson.DatabindCodec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.ActionProfile.Action;
import org.folio.rest.jaxrs.model.ActionProfile.FolioRecord;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.MatchProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.ProfileSnapshotUtil;

/**
 * Stateless queries over the job profile snapshot tree of a job execution:
 * which kinds of actions and match profiles the job profile contains.
 */
@Log4j2
final class JobProfileSnapshotInspector {

  private JobProfileSnapshotInspector() {
  }

  static boolean hasCreateOrderAction(JobExecution jobExecution) {
    var snapshot = jobExecution.getJobProfileSnapshotWrapper();
    if (snapshot == null) {
      return false;
    }
    boolean found = snapshot.getChildSnapshotWrappers().stream()
      .filter(wrapper -> wrapper.getContentType() == ACTION_PROFILE)
      .anyMatch(JobProfileSnapshotInspector::isCreateOrderActionProfile);
    if (found) {
      log.debug("hasCreateOrderAction:: Event type for Order's logic set by jobExecutionId {} ", jobExecution.getId());
    }
    return found;
  }

  static boolean hasUpdateMarcAction(JobExecution jobExecution) {
    return hasAction(jobExecution, List.of(FolioRecord.MARC_BIBLIOGRAPHIC, FolioRecord.MARC_AUTHORITY,
      FolioRecord.MARC_HOLDINGS), Action.UPDATE);
  }

  static boolean hasUpdateInstanceAction(JobExecution jobExecution) {
    return hasAction(jobExecution, List.of(FolioRecord.INSTANCE), Action.UPDATE);
  }

  static boolean hasDeleteMarcAuthorityAction(JobExecution jobExecution) {
    return hasAction(jobExecution, List.of(FolioRecord.MARC_AUTHORITY), Action.DELETE);
  }

  static boolean hasCreateAuthorityAction(JobExecution jobExecution) {
    return hasAction(jobExecution, List.of(FolioRecord.AUTHORITY), Action.CREATE);
  }

  static boolean hasCreateOrUpdateItemOrHoldingsAction(JobExecution jobExecution, List<Record> parsedRecords) {
    var snapshot = jobExecution.getJobProfileSnapshotWrapper();
    var itemAndHoldings = List.of(FolioRecord.ITEM, FolioRecord.HOLDINGS);
    return (containsAction(snapshot, itemAndHoldings, Action.CREATE) || containsAction(snapshot, itemAndHoldings, Action.UPDATE))
      && !containsAction(snapshot, List.of(FolioRecord.INSTANCE), Action.CREATE)
      && !CollectionUtils.isEmpty(parsedRecords)
      && parsedRecords.getFirst().getRecordType() == MARC_BIB;
  }

  static boolean hasCreateInstanceActionWithoutPrecedingDelete999Field(JobExecution jobExecution) {
    return containsCreateInstanceActionWithoutPrecedingDelete999Field(jobExecution.getJobProfileSnapshotWrapper());
  }

  static boolean hasCreateMarcHoldingsAction(JobExecution jobExecution) {
    return anyDescendant(jobExecution.getJobProfileSnapshotWrapper(), child ->
      child.getContentType() == ACTION_PROFILE
        && actionProfileMatches(child, List.of(FolioRecord.HOLDINGS), Action.CREATE)
        && isMarcHoldingsMapping(child));
  }

  static boolean hasMarcAuthorityMatchProfile(JobExecution jobExecution) {
    return anyDescendant(jobExecution.getJobProfileSnapshotWrapper(), child ->
      child.getContentType() == MATCH_PROFILE && isMarcAuthorityMatchProfile(child));
  }

  static boolean shouldRemoveSubfield9(ProfileSnapshotWrapper profileSnapshot) {
    return anyDescendant(profileSnapshot, child ->
      child.getContentType() == ACTION_PROFILE
        && TRUE.equals(contentAs(child, ActionProfile.class).getRemove9Subfields()));
  }

  private static boolean hasAction(JobExecution jobExecution, List<FolioRecord> entityTypes, Action action) {
    return containsAction(jobExecution.getJobProfileSnapshotWrapper(), entityTypes, action);
  }

  private static boolean containsAction(ProfileSnapshotWrapper profileSnapshot, List<FolioRecord> entityTypes, Action action) {
    return anyDescendant(profileSnapshot, child ->
      child.getContentType() == ACTION_PROFILE && actionProfileMatches(child, entityTypes, action));
  }

  /**
   * Depth-first search of the descendants of {@code root} (the root itself is not tested).
   */
  private static boolean anyDescendant(ProfileSnapshotWrapper root, Predicate<ProfileSnapshotWrapper> predicate) {
    for (ProfileSnapshotWrapper child : root.getChildSnapshotWrappers()) {
      if (predicate.test(child) || anyDescendant(child, predicate)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsCreateInstanceActionWithoutPrecedingDelete999Field(ProfileSnapshotWrapper profileSnapshot) {
    List<ProfileSnapshotWrapper> childWrappers = profileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == ACTION_PROFILE
        && actionProfileMatches(childWrapper, List.of(FolioRecord.INSTANCE), Action.CREATE)) {
        if (childWrapper.getReactTo() != NON_MATCH && !hasPrecedingDelete999Modification(childWrappers, childWrapper)) {
          return true;
        }
      } else if (containsCreateInstanceActionWithoutPrecedingDelete999Field(childWrapper)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasPrecedingDelete999Modification(List<ProfileSnapshotWrapper> siblings,
                                                           ProfileSnapshotWrapper createInstanceWrapper) {
    return siblings.stream()
      .anyMatch(wrapper -> wrapper.getContentType() == ACTION_PROFILE
        && actionProfileMatches(wrapper, List.of(FolioRecord.MARC_BIBLIOGRAPHIC), Action.MODIFY)
        && wrapper.getOrder() != null && createInstanceWrapper.getOrder() != null
        && wrapper.getOrder() < createInstanceWrapper.getOrder()
        && ProfileSnapshotUtil.containsDelete999FieldMappingDetail(wrapper));
  }

  private static boolean isCreateOrderActionProfile(ProfileSnapshotWrapper wrapper) {
    Map<String, String> content = DatabindCodec.mapper().convertValue(wrapper.getContent(), HashMap.class);
    return content.get(FOLIO_RECORD).equals(ORDER_TYPE) && content.get(ACTION_FIELD).equals(CREATE_ACTION);
  }

  private static boolean actionProfileMatches(ProfileSnapshotWrapper wrapper, List<FolioRecord> records, Action action) {
    ActionProfile actionProfile = contentAs(wrapper, ActionProfile.class);
    return records.contains(actionProfile.getFolioRecord()) && actionProfile.getAction() == action;
  }

  private static boolean isMarcAuthorityMatchProfile(ProfileSnapshotWrapper wrapper) {
    MatchProfile matchProfile = contentAs(wrapper, MatchProfile.class);
    return matchProfile.getExistingRecordType() == EntityType.MARC_AUTHORITY
      && matchProfile.getIncomingRecordType() == EntityType.MARC_AUTHORITY;
  }

  private static boolean isMarcHoldingsMapping(ProfileSnapshotWrapper actionProfileWrapper) {
    List<ProfileSnapshotWrapper> childWrappers = actionProfileWrapper.getChildSnapshotWrappers();
    if (childWrappers != null && !childWrappers.isEmpty() && childWrappers.getFirst() != null) {
      MappingProfile mappingProfile = contentAs(childWrappers.getFirst(), MappingProfile.class);
      return mappingProfile.getExistingRecordType() == EntityType.HOLDINGS
        && mappingProfile.getIncomingRecordType() == EntityType.MARC_HOLDINGS;
    }
    return false;
  }

  private static <T> T contentAs(ProfileSnapshotWrapper wrapper, Class<T> type) {
    return DatabindCodec.mapper().convertValue(wrapper.getContent(), type);
  }
}
