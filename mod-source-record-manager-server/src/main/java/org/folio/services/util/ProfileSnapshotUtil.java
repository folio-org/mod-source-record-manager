package org.folio.services.util;

import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.MODIFY;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

import io.vertx.core.json.jackson.DatabindCodec;

import java.util.List;

import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

public final class ProfileSnapshotUtil {

  private static final String TAG_999 = "999";
  private static final String WILDCARD = "*";

  private ProfileSnapshotUtil() {
  }

  /**
   * Checks whether the given action profile wrapper contains a child MAPPING_PROFILE
   * with a MODIFY marc mapping option that includes a DELETE action mapping detail definition targeting the 999 field
   * with wildcard indicators and wildcard subfield.
   *
   * @param actionWrapper action profile snapshot wrapper to inspect
   * @return {@code true} if such a mapping detail is found, otherwise {@code false}
   */
  public static boolean containsDelete999FieldMappingDetail(ProfileSnapshotWrapper actionWrapper) {
    return actionWrapper.getChildSnapshotWrappers().stream()
      .filter(wrapper -> wrapper.getContentType() == MAPPING_PROFILE)
      .map(wrapper -> DatabindCodec.mapper().convertValue(wrapper.getContent(), MappingProfile.class))
      .anyMatch(mappingProfile -> mappingProfile.getMappingDetails() != null
        && mappingProfile.getMappingDetails().getMarcMappingOption() == MODIFY
        && mappingProfile.getMappingDetails().getMarcMappingDetails() != null
        && containsDelete999FieldMarcMappingDetail(mappingProfile.getMappingDetails().getMarcMappingDetails()));
  }

  /**
   * Checks whether the given list of {@link MarcMappingDetail} contains a DELETE action mapping detail
   * targeting the 999 field with wildcard indicators and wildcard subfield.
   * The method checks wildcard value for indicators and subfield definition because UI does not allow
   * to specify 'f' indicators for mapping detail definition for the 999 field. Consequently, according to use case,
   * the wildcard definition should be used to delete 999ff field.
   *
   * @param marcMappingDetails list of MARC mapping details to inspect
   * @return {@code true} if a matching DELETE mapping detail for the 999 field is found, otherwise {@code false}
   */
  public static boolean containsDelete999FieldMarcMappingDetail(List<MarcMappingDetail> marcMappingDetails) {
    return marcMappingDetails.stream()
      .anyMatch(detail -> detail.getAction() == MarcMappingDetail.Action.DELETE
        && detail.getField() != null
        && TAG_999.equals(detail.getField().getField())
        && WILDCARD.equals(detail.getField().getIndicator1())
        && WILDCARD.equals(detail.getField().getIndicator2())
        && detail.getField().getSubfields() != null
        && detail.getField().getSubfields().stream()
          .anyMatch(subfield -> WILDCARD.equals(subfield.getSubfield())));
  }
}
