package org.folio.services.util;

import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import io.vertx.core.json.JsonObject;

import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Test;

public class ProfileSnapshotUtilTest {

  private static final String TAG_999 = "999";
  private static final String WILDCARD = "*";

  @Test
  public void shouldReturnTrueIfActionWrapperContainsMappingProfileContainingDelete999FieldDetail() {
    ProfileSnapshotWrapper actionWrapper = actionWrapperWith(
      mappingWrapper(modifyMarcMappingProfileWith(delete999FieldMarcMappingDetail()))
    );

    assertTrue(ProfileSnapshotUtil.containsDelete999FieldMappingDetail(actionWrapper));
  }

  @Test
  public void shouldReturnFalseIfNoMappingProfileChildrenExist() {
    ProfileSnapshotWrapper actionWrapper = new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withChildSnapshotWrappers(Collections.emptyList());

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMappingDetail(actionWrapper));
  }

  @Test
  public void shouldReturnFalseIfMarcMappingOptionIsNotModify() {
    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(MappingDetail.MarcMappingOption.UPDATE)
        .withMarcMappingDetails(List.of(delete999FieldMarcMappingDetail())));
    ProfileSnapshotWrapper actionWrapper = actionWrapperWith(mappingWrapper(mappingProfile));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMappingDetail(actionWrapper));
  }

  @Test
  public void shouldReturnFalseIfMarcMappingDetailsIsNull() {
    MappingProfile mappingProfile = new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY)
        .withMarcMappingDetails(null));
    ProfileSnapshotWrapper actionWrapper = actionWrapperWith(mappingWrapper(mappingProfile));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMappingDetail(actionWrapper));
  }

  @Test
  public void shouldReturnTrueIfMarcMappingDetailsContainDelete999WildcardDetail() {
    assertTrue(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(
      List.of(delete999FieldMarcMappingDetail())));
  }

  @Test
  public void shouldReturnFalseIfMarcMappingDetailsListIsEmpty() {
    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(
      Collections.emptyList()));
  }

  @Test
  public void shouldReturnFalseIfMarcMappingDetailActionIsNotDelete() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField()
        .withField(TAG_999)
        .withIndicator1(WILDCARD)
        .withIndicator2(WILDCARD)
        .withSubfields(List.of(new MarcSubfield().withSubfield(WILDCARD))));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  @Test
  public void shouldReturnFalseIfMarcMappingDetailFieldIsNull() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(null);

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  @Test
  public void shouldReturnFalseIfMarcFieldTagIsNot999() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField("900")
        .withIndicator1(WILDCARD)
        .withIndicator2(WILDCARD)
        .withSubfields(List.of(new MarcSubfield().withSubfield(WILDCARD))));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  @Test
  public void shouldReturnFalseIfIndicator1IsNotWildcard() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField(TAG_999)
        .withIndicator1("1")
        .withIndicator2(WILDCARD)
        .withSubfields(List.of(new MarcSubfield().withSubfield(WILDCARD))));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  @Test
  public void shouldReturnFalseIfIndicator2IsNotWildcard() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField(TAG_999)
        .withIndicator1(WILDCARD)
        .withIndicator2("1")
        .withSubfields(List.of(new MarcSubfield().withSubfield(WILDCARD))));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  @Test
  public void shouldReturnFalseIfSubfieldValueIsNotWildcard() {
    MarcMappingDetail detail = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField(TAG_999)
        .withIndicator1(WILDCARD)
        .withIndicator2(WILDCARD)
        .withSubfields(List.of(new MarcSubfield().withSubfield("a"))));

    assertFalse(ProfileSnapshotUtil.containsDelete999FieldMarcMappingDetail(List.of(detail)));
  }

  private ProfileSnapshotWrapper actionWrapperWith(ProfileSnapshotWrapper children) {
    return new ProfileSnapshotWrapper()
      .withContentType(ACTION_PROFILE)
      .withChildSnapshotWrappers(List.of(children));
  }

  private ProfileSnapshotWrapper mappingWrapper(MappingProfile mappingProfile) {
    return new ProfileSnapshotWrapper()
      .withContentType(MAPPING_PROFILE)
      .withContent(JsonObject.mapFrom(mappingProfile).getMap());
  }

  private MappingProfile modifyMarcMappingProfileWith(MarcMappingDetail details) {
    return new MappingProfile()
      .withMappingDetails(new MappingDetail()
        .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY)
        .withMarcMappingDetails(List.of(details)));
  }

  private MarcMappingDetail delete999FieldMarcMappingDetail() {
    return new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField()
        .withField(TAG_999)
        .withIndicator1(WILDCARD)
        .withIndicator2(WILDCARD)
        .withSubfields(List.of(new MarcSubfield().withSubfield(WILDCARD))));
  }

}
