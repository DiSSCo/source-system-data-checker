package eu.dissco.sourcesystemdatachecker.repository;

import static eu.dissco.sourcesystemdatachecker.TestUtils.CREATED;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MAPPER;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_URI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.sourcesystemdatachecker.database.jooq.tables.DigitalMediaObject.DIGITAL_MEDIA_OBJECT;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaRecord;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MediaRepositoryIT extends BaseRepositoryIT {

  private MediaRepository mediaRepository;


  @BeforeEach
  void setup() {
    mediaRepository = new MediaRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(DIGITAL_MEDIA_OBJECT).execute();
  }

  @Test
  void testGetMedia() {
    // Given
    var expected = givenDigitalMediaRecord();
    insertMedia(expected);

    // When
    var result = mediaRepository.getExistingDigitalMedia(Set.of(MEDIA_URI_1));

    // Then
    assertThat(result).isEqualTo(Map.of(MEDIA_URI_1, expected));
  }

  @Test
  void testUpdateLastChecked() {
    // Given
    insertMedia(givenDigitalMediaRecord());

    // When
    mediaRepository.updateLastChecked(Set.of(MEDIA_DOI_1));
    var result = context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.eq(MEDIA_DOI_1))
        .fetchOne(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.class);

    // Then
    assertThat(result).isAfter(CREATED);
  }

  private void insertMedia(DigitalMediaRecord digitalMediaRecord) {
    context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, digitalMediaRecord.id())
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.attributes().getOdsFdoType())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, 1)
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, CREATED)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, CREATED)
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                MAPPER.valueToTree(digitalMediaRecord.attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaRecord.originalAttributes().toString()))
        .set(DIGITAL_MEDIA_OBJECT.MODIFIED, CREATED)
        .execute();
  }


}
