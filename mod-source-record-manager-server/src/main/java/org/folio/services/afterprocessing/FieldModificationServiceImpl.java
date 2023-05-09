package org.folio.services.afterprocessing;

import static org.folio.services.afterprocessing.AdditionalFieldsUtil.modifyDataFieldsForMarcRecord;

import io.vertx.core.Future;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.folio.LinkingRuleDto;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.marc4j.marc.DataField;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class FieldModificationServiceImpl implements FieldModificationService {
  
  private static final char SUBFIELD_9 = '9';

  private final MappingParametersProvider mappingParametersProvider;

  public FieldModificationServiceImpl(MappingParametersProvider mappingParametersProvider) {
    this.mappingParametersProvider = mappingParametersProvider;
  }

  @Override
  public Future<List<Record>> remove9Subfields(String jobExecutionId, List<Record> folioRecords, OkapiConnectionParams okapiParams) {
    log.trace("remove9Subfields:: called for job {}", jobExecutionId);
    return mappingParametersProvider.get(jobExecutionId, okapiParams).map(mappingParameters -> {
      if (mappingParameters.getLinkingRules() == null || mappingParameters.getLinkingRules().isEmpty()) {
        log.warn("Linking rules can't be 'null' for $9 removal.");
        throw new IllegalStateException("Can't remove $9 subfields without linking rules.");
      }

      log.trace("remove9Subfields:: mappingParameters retrieved for job {} with linkingRules count {}", jobExecutionId,
        mappingParameters.getLinkingRules().size());
      var linkableFields = mappingParameters.getLinkingRules().stream()
        .map(LinkingRuleDto::getBibField)
        .collect(Collectors.toList());

      folioRecords.stream()
        .filter(folioRecord -> Record.RecordType.MARC_BIB.equals(folioRecord.getRecordType()))
        .forEach(folioRecord -> modifyDataFieldsForMarcRecord(folioRecord, linkableFields, this::removeSubfields));

      return folioRecords;
    });
  }
  
  private void removeSubfields(DataField dataField) {
    var subfields9 = dataField.getSubfields().stream()
      .filter(subfield -> SUBFIELD_9 == subfield.getCode())
      .collect(Collectors.toList());
    subfields9.forEach(dataField::removeSubfield);
  }
}
