package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseMappingRulesMigrationTest {

  private static final String TENANT_ID = "test";
  private static final String UPDATED_RULES = "updated rules";
  private static final String TAG = "tag";
  private static final String TARGET = "target";
  private static final String FEATURE_VERSION = "some-version";
  private static final String DESCRIPTION = "some-description";

  private final MappingRuleService mappingRuleService = mock(MappingRuleService.class);
  private final TestBaseMappingRulesMigration migration = new TestBaseMappingRulesMigration(MARC_AUTHORITY,
    FEATURE_VERSION, DESCRIPTION, mappingRuleService);

  @Test
  public void migrateShouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(MARC_AUTHORITY, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).internalUpdate(anyString(), eq(MARC_AUTHORITY), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
    });
  }

  @Test
  public void migrateShouldUpdateRulesIfRulesExist() {
    when(mappingRuleService.get(MARC_AUTHORITY, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.of(new JsonObject())));
    when(mappingRuleService.internalUpdate(UPDATED_RULES, MARC_AUTHORITY, TENANT_ID))
      .thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(UPDATED_RULES, MARC_AUTHORITY, TENANT_ID);
      Assert.assertTrue(ar.succeeded());
    });
  }

  @Test
  public void getFeatureVersionShouldReturnCorrectFeatureVersion() {
    Assert.assertEquals(FEATURE_VERSION, migration.getFeatureVersion());
  }

  @Test
  public void getDescriptionShouldReturnCorrectDescription() {
    Assert.assertEquals(DESCRIPTION, migration.getDescription());
  }

  @Test
  public void addFieldIfNotExistsShouldAddIfFieldDoesNotExist() {
    var rules = spy(new JsonObject());
    var field = new JsonObject();
    migration.addFieldIfNotExists(rules, TAG, field);
    verify(rules).put(TAG, JsonArray.of(field));
  }

  @Test
  public void addFieldIfNotExistsShouldNotAddFieldIfFieldExists() {
    var rules = spy(new JsonObject().put(TAG, new JsonObject()));
    var field = new JsonObject();
    migration.addFieldIfNotExists(rules, TAG, field);
    verify(rules, never()).put(TAG, JsonArray.of(field));
  }

  @Test
  public void createFieldShouldCreateField() {
    var target = "some target";
    var description = "some description";
    var subfields = new JsonArray();
    var rules = new JsonArray();
    var field = migration.createField(target, description, subfields, rules);
    Assert.assertEquals(target, field.getString(TARGET));
    Assert.assertEquals(description, field.getString("description"));
    Assert.assertEquals(subfields, field.getJsonArray("subfield"));
    Assert.assertEquals(rules, field.getJsonArray("rules"));
  }

  @Test
  public void sortRulesShouldSortRules() {
    var namedEventTag = "147";
    var namedEventFields = JsonArray.of(new JsonObject().put(TARGET, "namedEvent"));
    var generalSubdivisionTag = "180";
    var generalSubdivisionFields = JsonArray.of(new JsonObject().put(TARGET, "generalSubdivision"));
    var rules = new JsonObject();
    rules.put(generalSubdivisionTag, generalSubdivisionFields);
    rules.put(namedEventTag, namedEventFields);
    var sortedRules = migration.sortRules(rules);
    var sortedTags = sortedRules.stream()
      .map(Map.Entry::getKey)
      .toList();
    var sortedFields = sortedRules.stream()
      .map(Map.Entry::getValue)
      .toList();
    Assert.assertEquals(List.of(namedEventTag, generalSubdivisionTag), sortedTags);
    Assert.assertEquals(List.of(namedEventFields, generalSubdivisionFields), sortedFields);
  }

  private static final class TestBaseMappingRulesMigration extends BaseMappingRulesMigration {

    private TestBaseMappingRulesMigration(Record.RecordType recordType, String featureVersion, String description,
                                          MappingRuleService mappingRuleService) {
      super(recordType, featureVersion, description, mappingRuleService);
    }

    @Override
    protected String updateRules(JsonObject rules) {
      return UPDATED_RULES;
    }
  }
}
