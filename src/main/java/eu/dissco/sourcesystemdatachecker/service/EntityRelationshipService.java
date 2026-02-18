package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEventWithFilteredMedia;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EntityRelationshipService {

  private EntityRelationshipService(){
    // Utility class
  }


  private static final String DOI_PROXY = "https://doi.org/";
  private static final String HAS_MEDIA = "hasDigitalMedia";

  // Adds existing media ERs to existing specimen
  // Filters out tombstoned relationships for specimen (i.e. events that are not in the current event)
  public static DigitalSpecimenEventWithFilteredMedia processMediaRelationshipsForSpecimen(
      DigitalSpecimenRecord currentSpecimen,
      DigitalSpecimenEventWithFilteredMedia specimenEvent,
      Map<String, DigitalMediaRecord> currentMedia
  ) {
    var attributes = generateAttributes(currentSpecimen,
        specimenEvent.digitalSpecimenWrapper().attributes(),
        Stream.concat(
            specimenEvent.changedMedia().stream().map(ServiceUtils::getMediaUri),
            specimenEvent.unchangedMedia().stream().map(DigitalMediaRecord::accessURI)
        ).toList(),
        currentMedia
    );
    return new DigitalSpecimenEventWithFilteredMedia(
        specimenEvent.masList(),
        new DigitalSpecimenWrapper(
            specimenEvent.digitalSpecimenWrapper().physicalSpecimenId(),
            specimenEvent.digitalSpecimenWrapper().type(),
            attributes,
            specimenEvent.digitalSpecimenWrapper().originalAttributes()
        ),
        specimenEvent.unchangedMedia(),
        specimenEvent.changedMedia(),
        specimenEvent.forceMasSchedule(),
        specimenEvent.isDataFromSourceSystem());
  }

  public static DigitalSpecimenEvent processMediaRelationshipsForSpecimen(
      DigitalSpecimenRecord currentSpecimen,
      DigitalSpecimenEvent specimenEvent,
      Map<String, DigitalMediaRecord> currentMedia
  ) {
    var mediaUris = specimenEvent.digitalMediaEvents().stream()
        .map(ServiceUtils::getMediaUri).toList();
    var attributes = generateAttributes(currentSpecimen,
        specimenEvent.digitalSpecimenWrapper().attributes(),
        mediaUris, currentMedia);
    return new DigitalSpecimenEvent(
        specimenEvent.masList(),
        new DigitalSpecimenWrapper(
            specimenEvent.digitalSpecimenWrapper().physicalSpecimenId(),
            specimenEvent.digitalSpecimenWrapper().type(),
            attributes,
            specimenEvent.digitalSpecimenWrapper().originalAttributes()
        ),
        specimenEvent.digitalMediaEvents(),
        specimenEvent.forceMasSchedule(),
        specimenEvent.isDataFromSourceSystem()
    );
  }

  private static DigitalSpecimen generateAttributes(
      DigitalSpecimenRecord currentSpecimen,
      DigitalSpecimen attributes,
      List<String> mediaUris,
      Map<String, DigitalMediaRecord> currentMedia
  ) {
    if (currentSpecimen == null) {
      return attributes; // Specimen is new; new media ERs will be added by processor
    }
    var mediaIdMap = getExistingMediaIdMap(currentMedia);
    var ersToKeep = removeTombstonedErs(mediaUris,
        currentSpecimen.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
        mediaIdMap);
    attributes.setOdsHasEntityRelationships(ersToKeep);
    return attributes;
  }

  private static Map<String, String> getExistingMediaIdMap(
      Map<String, DigitalMediaRecord> currentMedia) {
    return currentMedia.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().id(),
            Entry::getKey
        ));
  }

  private static List<EntityRelationship> removeTombstonedErs(
      List<String> mediaUris, List<EntityRelationship> currentErs,
      Map<String, String> mediaIdMap) {
    // Get media URIs from this batch
    // If a media ER is NOT associated with a URI that is in this current batch, that means the relationship no longer exists
    return currentErs.stream().filter(
        er -> {
          if (!HAS_MEDIA.equals(er.getDwcRelationshipOfResource())) {
            // Not a media ER, so we keep it
            return true;
          }
          var mediaUri = mediaIdMap.get(
              er.getDwcRelatedResourceID().replace(DOI_PROXY, ""));
          return mediaUris.contains(mediaUri);
        }
    ).toList();
  }

}
