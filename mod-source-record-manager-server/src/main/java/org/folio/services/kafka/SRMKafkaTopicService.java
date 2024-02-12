package org.folio.services.kafka;

import org.folio.kafka.services.KafkaTopic;
import org.folio.services.kafka.support.SRMKafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class SRMKafkaTopicService {

  @Value("${di_completed.partitions}")
  private Integer diCompletedNumPartitions;

  @Value("${di_error.partitions}")
  private Integer diErrorNumPartitions;

  @Value("${di_srs_marc_authority_record_created.partitions}")
  private Integer diSrsMarcAuthorityRecordCreatedNumPartitions;

  @Value("${di_srs_marc_holdings_record_created.partitions}")
  private Integer diSrsMarcHoldingsRecordCreatedNumPartitions;

  @Value("${di_raw_records_chunk_parsed.partitions}")
  private Integer diRawRecordsChunkParsedNumPartitions;

  @Value("${di_marc_for_update_received.partitions}")
  private Integer diMarcForUpdateReceivedNumPartitions;

  @Value("${di_marc_for_delete_received.partitions}")
  private Integer diMarcForDeleteReceivedNumPartitions;

  @Value("${di_marc_for_order_parsed.partitions}")
  private Integer diMarcOrderParsedNumPartitions;

  @Value("${di_marc_bib_record_parsed.partitions}")
  private Integer diMarcBibRecordParsedNumPartitions;

  @Value("${di_edifact_parsed.partitions}")
  private Integer diEdifactRecordParsedNumPartitions;

  public KafkaTopic[] createTopicObjects() {
    return new SRMKafkaTopic[] {
      new SRMKafkaTopic("DI_COMPLETED", diCompletedNumPartitions),
      new SRMKafkaTopic("DI_ERROR", diErrorNumPartitions),
      new SRMKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_CREATED", diSrsMarcAuthorityRecordCreatedNumPartitions),
      new SRMKafkaTopic("DI_SRS_MARC_HOLDING_RECORD_CREATED", diSrsMarcHoldingsRecordCreatedNumPartitions),
      new SRMKafkaTopic("DI_RAW_RECORDS_CHUNK_PARSED", diRawRecordsChunkParsedNumPartitions),
      new SRMKafkaTopic("DI_MARC_FOR_UPDATE_RECEIVED", diMarcForUpdateReceivedNumPartitions),
      new SRMKafkaTopic("DI_MARC_FOR_DELETE_RECEIVED", diMarcForDeleteReceivedNumPartitions),
      new SRMKafkaTopic("DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED", diMarcOrderParsedNumPartitions),
      new SRMKafkaTopic("DI_INCOMING_MARC_BIB_RECORD_PARSED", diMarcBibRecordParsedNumPartitions),
      new SRMKafkaTopic("DI_INCOMING_EDIFACT_RECORD_PARSED", diEdifactRecordParsedNumPartitions)
    };
  }
}
