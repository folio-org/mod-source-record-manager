package org.folio.dao;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.dao.util.JobExecutionDBConstants.COMPLETED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.FILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.HRID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.IS_DELETED_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_HIDDEN_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.SUBORDINATION_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.UI_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.USER_ID_FIELD;

public class JobExecutionFilter {
  public static final String LIKE = "LIKE";
  public static final String ILIKE = "ILIKE";
  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private List<JobExecution.Status> statusAny;
  private List<String> profileIdNotAny;
  private JobExecution.Status statusNot;
  private List<JobExecution.UiStatus> uiStatusAny;
  private String hrIdPattern;
  private String fileNamePattern;
  private List<String> fileNameNotAny;
  private List<String> profileIdAny;
  private List<JobExecutionDto.SubordinationType> subordinationTypeNotAny;
  private String userId;
  private Date completedAfter;
  private Date completedBefore;

  public JobExecutionFilter withStatusAny(List<JobExecution.Status> statusAny) {
    this.statusAny = statusAny;
    return this;
  }

  public JobExecutionFilter withProfileIdNotAny(List<String> profileIdNotAny) {
    this.profileIdNotAny = profileIdNotAny;
    return this;
  }

  public JobExecutionFilter withStatusNot(JobExecution.Status statusNot) {
    this.statusNot = statusNot;
    return this;
  }

  public JobExecutionFilter withUiStatusAny(List<JobExecution.UiStatus> uiStatusAny) {
    this.uiStatusAny = uiStatusAny;
    return this;
  }

  public JobExecutionFilter withHrIdPattern(String hrIdPattern) {
    this.hrIdPattern = hrIdPattern;
    return this;
  }

  public JobExecutionFilter withFileNamePattern(String fileNamePattern) {
    this.fileNamePattern = fileNamePattern;
    return this;
  }

  public JobExecutionFilter withFileNameNotAny(List<String> fileNameNotAny) {
    this.fileNameNotAny = fileNameNotAny;
    return this;
  }

  public JobExecutionFilter withProfileIdAny(List<String> profileIdAny) {
    this.profileIdAny = profileIdAny;
    return this;
  }

  public JobExecutionFilter withSubordinationTypeNotAny(List<JobExecutionDto.SubordinationType> subordinationTypeNotAny) {
    this.subordinationTypeNotAny = subordinationTypeNotAny;
    return this;
  }

  public JobExecutionFilter withUserId(String userId) {
    this.userId = userId;
    return this;
  }

  public JobExecutionFilter withCompletedAfter(Date completedAfter) {
    this.completedAfter = completedAfter;
    return this;
  }

  public JobExecutionFilter withCompletedBefore(Date completedBefore) {
    this.completedBefore = completedBefore;
    return this;
  }

  public String buildCriteria() {
    StringBuilder conditionBuilder = new StringBuilder("TRUE");
    if (isNotEmpty(statusAny)) {
      List<String> statuses = statusAny.stream()
        .map(JobExecution.Status::toString)
        .toList();

      addCondition(conditionBuilder, buildInCondition(STATUS_FIELD, statuses));
    }
    if (isNotEmpty(profileIdNotAny)) {
      addCondition(conditionBuilder, buildNotInCondition(JOB_PROFILE_ID_FIELD, profileIdNotAny));
    }
    if (statusNot != null) {
      addCondition(conditionBuilder, buildNotEqualCondition(STATUS_FIELD, statusNot.toString()));
    }
    if (isNotEmpty(uiStatusAny)) {
      List<String> uiStatuses = uiStatusAny.stream()
        .map(JobExecution.UiStatus::toString)
        .toList();

      addCondition(conditionBuilder, buildInCondition(UI_STATUS_FIELD, uiStatuses));
    }
    if (isNotEmpty(hrIdPattern) && isNotEmpty(fileNamePattern)) {
      conditionBuilder.append(String.format(" AND (%s OR %s)", buildCaseSensitiveLikeCondition(HRID_FIELD, hrIdPattern),
        buildCaseInsensitiveLikeCondition(FILE_NAME_FIELD, fileNamePattern)));
    } else {
      if (isNotEmpty(hrIdPattern)) {
        addCondition(conditionBuilder, buildCaseSensitiveLikeCondition(HRID_FIELD, hrIdPattern));
      }
      if (isNotEmpty(fileNamePattern)) {
        addCondition(conditionBuilder, buildCaseInsensitiveLikeCondition(FILE_NAME_FIELD, fileNamePattern));
      }
    }
    if (isNotEmpty(fileNameNotAny)) {
      addCondition(conditionBuilder, buildNotInCondition(FILE_NAME_FIELD, fileNameNotAny));
    }
    if (isNotEmpty(profileIdAny)) {
      addCondition(conditionBuilder, buildInCondition(JOB_PROFILE_ID_FIELD, profileIdAny));
    }
    if (isNotEmpty(subordinationTypeNotAny)) {
      List<String> subordinationTypes = subordinationTypeNotAny.stream().map(JobExecutionDto.SubordinationType::value).toList();
      addCondition(conditionBuilder, buildNotInCondition(SUBORDINATION_TYPE_FIELD, subordinationTypes));
    }
    if (isNotEmpty(userId)) {
      addCondition(conditionBuilder, buildEqualCondition(USER_ID_FIELD, userId));
    }
    if (completedAfter != null) {
      addCondition(conditionBuilder, buildGreaterThanOrEqualCondition(COMPLETED_DATE_FIELD, formatter.format(completedAfter)));
    }
    if (completedBefore != null) {
      addCondition(conditionBuilder, buildLessThanOrEqualCondition(COMPLETED_DATE_FIELD, formatter.format(completedBefore)));
    }
    addCondition(conditionBuilder, buildNotBoolCondition(JOB_PROFILE_HIDDEN_FIELD));
    addCondition(conditionBuilder, buildNotBoolCondition(IS_DELETED_FIELD));
    return conditionBuilder.toString();
  }

  private void addCondition(StringBuilder conditionBuilder, String condition) {
    conditionBuilder.append(" AND ").append(condition);
  }

  private String buildNotBoolCondition(String columnName) {
    return String.format("NOT %s", columnName);
  }

  private String buildInCondition(String columnName, List<String> values) {
    String preparedValues = values.stream()
      .map(s -> String.format("'%s'", s))
      .collect(Collectors.joining(", "));

    return String.format("%s IN (%s)", columnName, preparedValues);
  }

  private String buildNotInCondition(String columnName, List<String> values) {
    String preparedValues = values.stream()
      .map(s -> String.format("'%s'", s))
      .collect(Collectors.joining(", "));

    return String.format("%s NOT IN (%s)", columnName, preparedValues);
  }

  private String buildEqualCondition(String columnName, String value) {
    return String.format("%s = '%s'", columnName, value);
  }

  private String buildNotEqualCondition(String columnName, String value) {
    return String.format("%s <> '%s'", columnName, value);
  }

  private String buildGreaterThanOrEqualCondition(String columnName, String value) {
    return String.format("%s >= '%s'", columnName, value);
  }

  private String buildLessThanOrEqualCondition(String columnName, String value) {
    return String.format("%s <= '%s'", columnName, value);
  }

  private String buildCaseSensitiveLikeCondition(String columnName, String pattern) {
    return buildLikeCondition(columnName, pattern, true);
  }

  private String buildCaseInsensitiveLikeCondition(String columnName, String pattern) {
    return buildLikeCondition(columnName, pattern, false);
  }

  private String buildLikeCondition(String columnName, String pattern, boolean isCaseSensitive) {
    String preparedLikePattern = pattern.replace("*", "%");
    String likeOperator = isCaseSensitive ? LIKE : ILIKE;
    return String.format("%s::text %s '%s'", columnName, likeOperator, preparedLikePattern);
  }

}
