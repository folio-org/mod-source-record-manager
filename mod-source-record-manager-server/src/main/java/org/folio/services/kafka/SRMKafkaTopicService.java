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

  @Value("${di_parsed_records_chunk_saved.partitions}")
  private Integer diParsedRecordsChunkSavedNumPartitions;

  @Value("${di_raw_records_chunk_parsed.partitions}")
  private Integer diRawRecordsChunkParsedNumPartitions;

  public KafkaTopic[] createTopicObjects() {
    var diCompleteTopic = new SRMKafkaTopic("DI_COMPLETE", diCompleteNumPartitions);
    var diError = new SRMKafkaTopic("DI_ERROR", diErrorNumPartitions);
    var diParsedRecordsChunkSaved = new SRMKafkaTopic("DI_PARSED_RECORDS_CHUNK_SAVED", diParsedRecordsChunkSavedNumPartitions);
    var diRawRecordsChunkParsed = new SRMKafkaTopic("DI_RAW_RECORDS_CHUNK_PARSED", diRawRecordsChunkParsedNumPartitions);

    return new SRMKafkaTopic[] {diCompleteTopic, diError, diParsedRecordsChunkSaved, diRawRecordsChunkParsed};
  }
}
