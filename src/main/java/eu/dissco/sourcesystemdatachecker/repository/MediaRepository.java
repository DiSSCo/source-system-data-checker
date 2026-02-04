package eu.dissco.sourcesystemdatachecker.repository;

import static eu.dissco.sourcesystemdatachecker.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.schema.DigitalMedia;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MediaRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  // Maps Media URI to its DOI
  public Map<String, DigitalMediaRecord> getExistingDigitalMedia(Set<String> mediaURIs) {
    return context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaURIs))
        .fetch(this::mapToDigitalMediaRecord)
        .stream()
        .collect(Collectors.toMap(
            DigitalMediaRecord::accessURI,
            Function.identity()
        ));
  }

  private DigitalMediaRecord mapToDigitalMediaRecord(Record dbRecord) {
    try {
      return new DigitalMediaRecord(
          dbRecord.get(DIGITAL_MEDIA_OBJECT.ID),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.MEDIA_URL),
          mapper.readValue(dbRecord.get(DIGITAL_MEDIA_OBJECT.DATA).data(), DigitalMedia.class),
          mapper.readTree(dbRecord.get(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data()), null);
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord, e);
      return null;
    }
  }

  public void updateLastChecked(List<String> currentDigitalMedia) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMedia))
        .execute();
  }


}
