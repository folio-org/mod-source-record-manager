package org.folio.services.kafka;

import org.folio.kafka.services.KafkaTopic;
import org.folio.services.kafka.support.SRMKafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@Service
@PropertySource(value = "kafka.properties")
public class SRMKafkaTopicService {

  @Value("${di_complete.partitions}")
  private Integer diCompleteNumPartitions;

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

  @Value("${di_marc_for_order_created.partitions}")
  private Integer diMarcOrderCreatedNumPartitions;

  public KafkaTopic[] createTopicObjects() {
    var diCompleteTopic = new SRMKafkaTopic("DI_COMPLETE", diCompleteNumPartitions);
    var diError = new SRMKafkaTopic("DI_ERROR", diErrorNumPartitions);
    var diSrsMarcAuthorityCreated = new SRMKafkaTopic("DI_SRS_MARC_AUTHORITY_RECORD_CREATED", diSrsMarcAuthorityRecordCreatedNumPartitions);
    var diSrsMarcHoldingsCreated = new SRMKafkaTopic("DI_SRS_MARC_HOLDINGS_RECORD_CREATED",
      diSrsMarcHoldingsRecordCreatedNumPartitions);
    var diRawRecordsChunkParsed = new SRMKafkaTopic("DI_RAW_RECORDS_CHUNK_PARSED", diRawRecordsChunkParsedNumPartitions);
    var diMarcForUpdateReceived = new SRMKafkaTopic("DI_MARC_FOR_UPDATE_RECEIVED",
      diMarcForUpdateReceivedNumPartitions);
    var diMarcForDeleteReceived = new SRMKafkaTopic("DI_MARC_FOR_DELETE_RECEIVED",
      diMarcForDeleteReceivedNumPartitions);

    var diMarcBibOrderCreated = new SRMKafkaTopic("DI_MARC_BIB_FOR_ORDER_CREATED", diMarcOrderCreatedNumPartitions);

    return new SRMKafkaTopic[] {diCompleteTopic, diError, diSrsMarcAuthorityCreated,
                                diSrsMarcHoldingsCreated, diRawRecordsChunkParsed,
                                diMarcForUpdateReceived, diMarcForDeleteReceived, diMarcBibOrderCreated};
  }
}
