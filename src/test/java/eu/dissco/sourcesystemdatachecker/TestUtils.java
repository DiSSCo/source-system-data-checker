package eu.dissco.sourcesystemdatachecker;

import static eu.dissco.sourcesystemdatachecker.configuration.ApplicationConfiguration.DATE_STRING;

import com.fasterxml.jackson.annotation.JsonSetter.Value;
import com.fasterxml.jackson.annotation.Nulls;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaWrapper;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.schema.DigitalMedia;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class TestUtils {

  private TestUtils() {
    // Utility class
  }

  public static final String PHYSICAL_ID_1 = "AVES_1";
  public static final String PHYSICAL_ID_2 = "AVES_2";
  public static final String MEDIA_URI_1 = "https://media.com/1";
  public static final String MEDIA_URI_2 = "https://media.com/2";
  public static final String SPECIMEN_DOI = "10.3535/AAA-AAA-AAA";
  public static final String MEDIA_DOI_1 = "10.3535/111-111-111";
  public static final String MEDIA_DOI_2 = "10.3535/222-222-222";
  public static final String CURRENT_VAL = "default";
  public static final String CHANGED_VAL = "changed";
  public static final Instant CREATED = Instant.parse("2022-11-01T09:59:24.000Z");

  public static final JsonMapper MAPPER = JsonMapper.builder()
      .findAndAddModules()
      .defaultDateFormat(new SimpleDateFormat(DATE_STRING))
      .defaultTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC))
      .withConfigOverride(List.class, cfg ->
          cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
      .withConfigOverride(Map.class, cfg ->
          cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
      .withConfigOverride(Set.class, cfg ->
          cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
      .build();

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord() {
    return givenDigitalSpecimenRecord(SPECIMEN_DOI, PHYSICAL_ID_1, Map.of());
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMedia() {
    return givenDigitalSpecimenRecord(SPECIMEN_DOI, PHYSICAL_ID_1,
        Map.of(MEDIA_URI_1, MEDIA_DOI_1));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String id,
      String physicalSpecimenId, Map<String, String> mediaUriIdMap) {
    return new DigitalSpecimenRecord(
        id, givenDigitalSpecimenWrapperWithMediaErs(physicalSpecimenId, false,
            new HashSet<>(mediaUriIdMap.values())), mediaUriIdMap.keySet());
  }


  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of());
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEventWithMedia() {
    return givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of(givenDigitalMediaEvent()));
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId,
      boolean specimenIsChanged, List<DigitalMediaEvent> mediaEvents) {
    return new DigitalSpecimenEvent(
        Set.of(),
        givenDigitalSpecimenWrapper(physicalSpecimenId, specimenIsChanged),
        mediaEvents,
        false,
        false,
        false);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(String physicalSpecimenId,
      boolean isChanged) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        "ods:DigitalSpecimen",
        new DigitalSpecimen(),
        givenOriginalAttributes(isChanged)
    );
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperWithMediaErs(
      String physicalSpecimenId,
      boolean isChanged, Set<String> mediaIds) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        "ods:DigitalSpecimen",
        new DigitalSpecimen().withOdsHasEntityRelationships(
            givenMediaEntityRelationships(mediaIds)),
        givenOriginalAttributes(isChanged)
    );
  }

  private static List<EntityRelationship> givenMediaEntityRelationships(Set<String> mediaIds) {
    return mediaIds.stream()
        .map(mediaId -> new EntityRelationship()
            .withDwcRelationshipOfResource("hasDigitalMedia")
            .withDwcRelatedResourceID(mediaId)
            .withOdsRelatedResourceURI(URI.create(mediaId)))
        .toList();
  }

  public static JsonNode givenOriginalAttributes(boolean changed) {
    return MAPPER.createObjectNode()
        .put("key", changed ? CHANGED_VAL : CURRENT_VAL);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord() {
    return givenDigitalMediaRecord(MEDIA_DOI_1, MEDIA_URI_1);
  }

  public static DigitalMediaRecord givenDigitalMediaRecord(String id, String uri) {
    return new DigitalMediaRecord(
        id,
        uri,
        givenDigitalMedia(uri),
        givenOriginalAttributes(false)
    );
  }

  public static DigitalMediaEvent givenDigitalMediaEvent() {
    return givenDigitalMediaEvent(MEDIA_URI_1, false);
  }

  public static DigitalMediaEvent givenDigitalMediaEvent(String uri, boolean mediaIsChanged) {
    return new DigitalMediaEvent(
        Set.of(),
        givenDigitalMediaWrapper(uri, mediaIsChanged),
        false
    );
  }

  public static DigitalMediaWrapper givenDigitalMediaWrapper(String uri, boolean mediaIsChanged) {
    return new DigitalMediaWrapper(
        "ods:DigtialMedia",
        givenDigitalMedia(uri),
        givenOriginalAttributes(mediaIsChanged)
    );
  }

  private static DigitalMedia givenDigitalMedia(String uri) {
    return new DigitalMedia().withAcAccessURI(uri);
  }


}
