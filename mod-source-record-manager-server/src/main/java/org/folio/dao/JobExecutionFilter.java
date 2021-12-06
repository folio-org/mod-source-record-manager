package org.folio.dao;

import org.folio.rest.jaxrs.model.JobExecution;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.dao.util.JobExecutionsColumns.COMPLETED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.FILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.HRID_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.JOB_PROFILE_ID_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.STATUS_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.UI_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.USER_ID_FIELD;

public class JobExecutionFilter {

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private List<JobExecution.Status> statusAny;
  private List<String> profileIdNotAny;
  private JobExecution.Status statusNot;
  private List<JobExecution.UiStatus> uiStatusAny;
  private String hrIdPattern;
  private String fileNamePattern;
  private String profileId;
  private String userId;
  private Date completedAfter;
  private Date completedBefore;

  public JobExecutionFilter withStatusAny(List<JobExecution.Status> statusIn) {
    this.statusAny = statusIn;
    return this;
  }

  public JobExecutionFilter withProfileIdNotAny(List<String> profileIdNotIn) {
    this.profileIdNotAny = profileIdNotIn;
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

  public JobExecutionFilter withProfileId(String profileId) {
    this.profileId = profileId;
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
        .collect(Collectors.toList());

      conditionBuilder.append(" AND ")
        .append(buildInCondition(STATUS_FIELD, statuses));
    }
    if (isNotEmpty(profileIdNotAny)) {
      conditionBuilder.append(" AND ")
        .append(buildNotInCondition(JOB_PROFILE_ID_FIELD, profileIdNotAny));
    }
    if (statusNot != null) {
      conditionBuilder
        .append(" AND ")
        .append(buildNotEqualCondition(STATUS_FIELD, statusNot.toString()));
    }
    if (isNotEmpty(uiStatusAny)) {
      List<String> uiStatuses = uiStatusAny.stream()
        .map(JobExecution.UiStatus::toString)
        .collect(Collectors.toList());

      conditionBuilder.append(" AND ")
        .append(buildInCondition(UI_STATUS_FIELD, uiStatuses));
    }
    if (isNotEmpty(hrIdPattern) && isNotEmpty(fileNamePattern)) {
      conditionBuilder.append(String.format(" AND (%s OR %s)", buildLikeCondition(HRID_FIELD, hrIdPattern),
        buildLikeCondition(FILE_NAME_FIELD, fileNamePattern)));
    } else {
      if (isNotEmpty(hrIdPattern)) {
        conditionBuilder.append(" AND ")
          .append(buildLikeCondition(HRID_FIELD, hrIdPattern));
      }
      if (isNotEmpty(fileNamePattern)) {
        conditionBuilder.append(" AND ")
          .append(buildLikeCondition(FILE_NAME_FIELD, fileNamePattern));
      }
    }
    if (isNotEmpty(profileId)) {
      conditionBuilder
        .append(" AND ")
        .append(buildEqualCondition(JOB_PROFILE_ID_FIELD, profileId));
    }
    if (isNotEmpty(userId)) {
      conditionBuilder
        .append(" AND ")
        .append(buildEqualCondition(USER_ID_FIELD, userId));
    }
    if (completedAfter != null) {
      conditionBuilder
        .append(" AND ")
        .append(buildGreaterThanOrEqualCondition(COMPLETED_DATE_FIELD, formatter.format(completedAfter)));
    }
    if (completedBefore != null) {
      conditionBuilder
        .append(" AND ")
        .append(buildLessThanOrEqualCondition(COMPLETED_DATE_FIELD, formatter.format(completedBefore)));
    }

    return conditionBuilder.toString();
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

  private String buildLikeCondition(String columnName, String pattern) {
    String preparedLikePattern = pattern.replace("*", "%");
    return String.format("%s::text LIKE '%s'", columnName, preparedLikePattern);
  }

}
