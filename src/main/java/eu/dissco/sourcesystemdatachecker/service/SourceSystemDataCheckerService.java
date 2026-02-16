package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEventWithFilteredMedia;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigitalSpecimens;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigtialMedia;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class SourceSystemDataCheckerService {

  private final SpecimenRepository specimenRepository;
  private final MediaRepository mediaRepository;
  private final RabbitMqPublisherService rabbitMqPublisherService;

  public void handleMessages(List<DigitalSpecimenEvent> events) {
    var uniqueEvents = removeDuplicatesInBatch(events);
    log.info("Received {} unique events", events.size());
    var specimenEventMap = uniqueEvents.stream().collect(Collectors.toMap(
        event -> event.digitalSpecimenWrapper().physicalSpecimenId(),
        Function.identity()
    ));
    var currentSpecimenRecords = getCurrentSpecimen(specimenEventMap);
    var currentMediaRecords = getCurrentMedia(specimenEventMap);
    var currentSpecimensWithMediaUris = pairSpecimensWithMedia(currentSpecimenRecords,
        currentMediaRecords);
    var filteredResults = processSpecimensAndMedia(specimenEventMap, currentSpecimensWithMediaUris,
        currentMediaRecords);
    var filteredSpecimenEvents = filteredResults.getLeft();
    var filteredMediaEvents = filteredResults.getRight();
    log.info("{} specimens are new or changed; {} specimens are unchanged",
        filteredSpecimenEvents.newOrChangedSpecimens().size(),
        filteredSpecimenEvents.unchangedSpecimens().size());
    log.info("{} media are changed and belong to an unchanged specimen",
        filteredMediaEvents.newOrChangedMedia());
    updateLastCheckedSpecimens(filteredSpecimenEvents.unchangedSpecimens().keySet());
    updateLastCheckedMedia(filteredMediaEvents.unchangedMedia());
    log.info("Successfully updated lastChecked for {} specimens and {} media",
        filteredSpecimenEvents.unchangedSpecimens().size(),
        filteredMediaEvents.unchangedMedia().size());
    publishChangedAndNewSpecimens(filteredSpecimenEvents.newOrChangedSpecimens());
    publishSpecimenOnlyEvents(filteredSpecimenEvents.changedSpecimensWithUnchangedMedia());
    publishedChangedMedia(filteredMediaEvents.newOrChangedMedia());
  }

  private void publishSpecimenOnlyEvents(
      Set<DigitalSpecimenEventWithFilteredMedia> specimensWithFilteredMedia) {
    var publishEvents = specimensWithFilteredMedia.stream()
        .map(SourceSystemDataCheckerService::removeMediaFromSpecimenEvent)
        .collect(Collectors.toSet());
    publishEvents.forEach(
        rabbitMqPublisherService::publishNameUsageEvent);
    log.info("Published {} specimen events to the specimen-specific name usage service",
        publishEvents.size());
  }


  private static DigitalSpecimenEvent removeMediaFromSpecimenEvent(
      DigitalSpecimenEventWithFilteredMedia event) {
    return new DigitalSpecimenEvent(
        event.masList(),
        event.digitalSpecimenWrapper(),
        List.of(),
        event.forceMasSchedule(),
        event.isDataFromSourceSystem(),
        false
    );
  }

  private void publishChangedAndNewSpecimens(Set<DigitalSpecimenEvent> digitalSpecimenEvents) {
    digitalSpecimenEvents.forEach(
        rabbitMqPublisherService::publishNameUsageEvent);
    log.info("Published {} specimen events to the name usage service",
        digitalSpecimenEvents.size());
  }

  private void publishedChangedMedia(Set<DigitalMediaEvent> digitalMediaEvents) {
    digitalMediaEvents.forEach(rabbitMqPublisherService::publishMediaEvent);
    log.info("Published {} digital media events to the processing service",
        digitalMediaEvents.size());
  }

  private void updateLastCheckedSpecimens(Set<String> unchangedRecords) {
    if (unchangedRecords.isEmpty()) {
      return;
    }
    specimenRepository.updateLastChecked(unchangedRecords);
  }

  private void updateLastCheckedMedia(Set<DigitalMediaRecord> unchangedRecords) {
    if (unchangedRecords.isEmpty()) {
      return;
    }
    var mediaIds = unchangedRecords.stream().map(DigitalMediaRecord::id)
        .collect(Collectors.toSet());
    mediaRepository.updateLastChecked(mediaIds);
  }

  private Pair<FilteredDigitalSpecimens, FilteredDigtialMedia> processSpecimensAndMedia(
      Map<String, DigitalSpecimenEvent> specimenEventMap,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords,
      Map<String, DigitalMediaRecord> currentMediaRecords
  ) {
    var processedSpecimens = filterChangedAndNewSpecimens(specimenEventMap, currentSpecimenRecords,
        currentMediaRecords);
    var processedMedia = filterChangedAndNewMedia(processedSpecimens, currentMediaRecords);
    return Pair.of(processedSpecimens, processedMedia);
  }

  /*
    Takes incoming specimen events and the corresponding records of the events that exist
    Returns a list of new specimens and changed specimens, filtering out unchanged specimens
    This list will be sent downstream to the ingestion process.
   */
  private FilteredDigitalSpecimens filterChangedAndNewSpecimens(
      Map<String, DigitalSpecimenEvent> specimenEventMap,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords,
      Map<String, DigitalMediaRecord> currentMediaRecords
  ) {
    if (currentSpecimenRecords.isEmpty()) {
      return new FilteredDigitalSpecimens(new HashSet<>(specimenEventMap.values()), Set.of(),
          Map.of());
    }
    var newOrChangedSpecimens = new HashSet<DigitalSpecimenEvent>();
    var changedSpecimensWithUnchangedMedia = new HashSet<DigitalSpecimenEventWithFilteredMedia>();
    var unchangedSpecimens = new HashMap<String, DigitalSpecimenEvent>();
    specimenEventMap.forEach((key, specimenEvent) -> {
      var currentSpecimenRecord = currentSpecimenRecords.get(key);
      if (currentSpecimenRecord == null) { // This is a new specimen
        newOrChangedSpecimens.add(specimenEvent);
      } else if (specimenIsChanged(specimenEvent,
          currentSpecimenRecord)) { // This is an updated specimen
        if (mediaForSpecimenAreChanged(specimenEvent, currentMediaRecords) ||
            specimenMediaEntityRelationshipsAreChanged(specimenEvent, currentSpecimenRecord)) {
          // This updated specimen has updated media or the media it is related to has changed
          newOrChangedSpecimens.add(specimenEvent);
        } else {
          // this specimen should be updated without changing media ERs. The media data has changed, but all media are present.
          changedSpecimensWithUnchangedMedia.add(
              filterMediaForSpecimen(specimenEvent, currentMediaRecords));
        }
      } else {
        unchangedSpecimens.put(currentSpecimenRecord.id(), specimenEvent);
      }
    });
    return new FilteredDigitalSpecimens(
        newOrChangedSpecimens, changedSpecimensWithUnchangedMedia, unchangedSpecimens
    );
  }

  private static DigitalSpecimenEventWithFilteredMedia filterMediaForSpecimen(
      DigitalSpecimenEvent specimenEvent, Map<String, DigitalMediaRecord> currentMediaRecords) {
    var changedMedia = new HashSet<DigitalMediaEvent>();
    var unchangedMedia = new HashSet<DigitalMediaRecord>();
    specimenEvent.digitalMediaEvents().forEach(mediaEvent -> {
      var currentMediaRecord = currentMediaRecords.get(getAccessUri(mediaEvent));
      updateMediaProcessingLists(mediaEvent, currentMediaRecord, changedMedia, unchangedMedia);
    });
    return new DigitalSpecimenEventWithFilteredMedia(
        specimenEvent.masList(),
        specimenEvent.digitalSpecimenWrapper(),
        unchangedMedia,
        changedMedia,
        specimenEvent.forceMasSchedule(),
        specimenEvent.isDataFromSourceSystem()
    );
  }

  // Returns true if the specimen event should go through the normal ingestion process
  // Returns false if the specimen event should ignore changes to media ERs,
  // as some media are unchanged and will be omitted from the event
  private static boolean mediaForSpecimenAreChanged(
      DigitalSpecimenEvent specimenEvent, Map<String, DigitalMediaRecord> currentMediaRecords) {
    if (specimenEvent.digitalMediaEvents().isEmpty()) {
      return true;
    }
    for (var mediaEvent : specimenEvent.digitalMediaEvents()) {
      var currentMediaRecord = currentMediaRecords.get(
          getAccessUri(mediaEvent));
      if (currentMediaRecord == null || mediaIsChanged(mediaEvent, currentMediaRecord)) {
        return true; // Specimen and media should go to normal ingestion
      }
    }
    // Changes to specimen should ignore media ER changes, as some of the media will be omitted from the event
    return false;
  }

  private static boolean specimenIsChanged(DigitalSpecimenEvent specimenEvent,
      DigitalSpecimenRecord currentSpecimenRecord) {
    return !Objects.equals(specimenEvent.digitalSpecimenWrapper().originalAttributes(),
        currentSpecimenRecord.digitalSpecimenWrapper().originalAttributes())
        || specimenMediaEntityRelationshipsAreChanged(specimenEvent, currentSpecimenRecord);
  }

  private static boolean specimenMediaEntityRelationshipsAreChanged(
      DigitalSpecimenEvent specimenEvent, DigitalSpecimenRecord currentSpecimenRecord) {
    var incomingMedia = specimenEvent.digitalMediaEvents().stream()
        .map(SourceSystemDataCheckerService::getAccessUri)
        .collect(Collectors.toSet());
    return !incomingMedia.equals(currentSpecimenRecord.mediaUris());
  }

  /*
  1. looks in unchanged specimens -> Check if media is changed
  2. looks in changedSpecimensWithUnchangedMedia -> determines which media are changed, which are unchanged
  3. Looks in changed specimens -> do nothing, these will be sent with the specimen
   */

  public FilteredDigtialMedia filterChangedAndNewMedia(
      FilteredDigitalSpecimens processedSpecimens,
      Map<String, DigitalMediaRecord> currentMediaRecords) {
    // All media will be sent with specimenEvent
    if (processedSpecimens.unchangedSpecimens().isEmpty()
        && processedSpecimens.changedSpecimensWithUnchangedMedia().isEmpty()) {
      return new FilteredDigtialMedia(Set.of(), Set.of());
    }
    var newOrChangedMedia = new HashSet<DigitalMediaEvent>();
    var unchangedMedia = new HashSet<DigitalMediaRecord>();
    // Check if unchanged specimens have updated media
    processedSpecimens.unchangedSpecimens().values().stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .forEach(mediaEvent -> {
          var currentMediaRecord = currentMediaRecords.get(getAccessUri(mediaEvent));
          updateMediaProcessingLists(mediaEvent, currentMediaRecord, newOrChangedMedia,
              unchangedMedia);
        });
    // If an updated specimen has a mix of changed and unchanged media, we use the sorting we did earlier
    processedSpecimens.changedSpecimensWithUnchangedMedia()
        .forEach(specimenEvent -> {
          newOrChangedMedia.addAll(specimenEvent.changedMedia());
          unchangedMedia.addAll(specimenEvent.unchangedMedia());
        });
    return new FilteredDigtialMedia(newOrChangedMedia, unchangedMedia);
  }

  private static void updateMediaProcessingLists(DigitalMediaEvent mediaEvent,
      DigitalMediaRecord currentMediaRecord, HashSet<DigitalMediaEvent> newOrChangedMedia,
      HashSet<DigitalMediaRecord> unchangedMedia) {
    if (mediaIsChanged(mediaEvent, currentMediaRecord)) {
      newOrChangedMedia.add(mediaEvent);
    } else {
      unchangedMedia.add(currentMediaRecord);
    }
  }


  private static String getAccessUri(DigitalMediaEvent event) {
    return event.digitalMediaWrapper().attributes().getAcAccessURI();
  }


  private static boolean mediaIsChanged(DigitalMediaEvent mediaEvent,
      DigitalMediaRecord mediaRecord) {
    return !Objects.equals(mediaEvent.digitalMediaWrapper().originalAttributes(),
        mediaRecord.originalAttributes());
  }

  /*
    Takes incoming specimen events
    Checks repository for specimens with (normalised) physical specimen IDs that match the events'
    Merges duplicate records
    Returns a map, where the key is the physical specimen ID
   */
  private List<DigitalSpecimenRecord> getCurrentSpecimen(
      Map<String, DigitalSpecimenEvent> eventMap) {
    return specimenRepository.getDigitalSpecimens(eventMap.keySet());
  }

  private Map<String, DigitalMediaRecord> getCurrentMedia(
      Map<String, DigitalSpecimenEvent> specimenEventMap) {

    var incomingMediaUris = specimenEventMap.values().stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .map(SourceSystemDataCheckerService::getAccessUri)
        .collect(Collectors.toSet());
    return mediaRepository.getExistingDigitalMedia(incomingMediaUris);
  }

  private Set<DigitalSpecimenEvent> removeDuplicatesInBatch(
      List<DigitalSpecimenEvent> events) {
    var uniqueSet = new LinkedHashSet<DigitalSpecimenEvent>();
    var uniqueMediaSet = new LinkedHashSet<String>();
    var map = events.stream()
        .collect(
            Collectors.groupingBy(event -> event.digitalSpecimenWrapper().physicalSpecimenId()));
    for (var entry : map.entrySet()) {
      if (entry.getValue().size() > 1) {
        log.warn("Found {} duplicate specimen in batch for id {}", entry.getValue().size(),
            entry.getKey());
        var specimenIsNotPublished = true;
        for (var duplicateSpecimenEvent : entry.getValue()) {
          if (specimenIsNotPublished) {
            addToUniqueSets(uniqueSet, duplicateSpecimenEvent, uniqueMediaSet);
            specimenIsNotPublished = false;
          } else {
            republishSpecimenEvent(duplicateSpecimenEvent);
          }
        }
      } else {
        addToUniqueSets(uniqueSet, entry.getValue().getFirst(), uniqueMediaSet);
      }
    }
    return uniqueSet;
  }

  private void republishSpecimenEvent(DigitalSpecimenEvent event) {
    rabbitMqPublisherService.republishEvent(event);
  }

  private static void addToUniqueSets(LinkedHashSet<DigitalSpecimenEvent> uniqueSet,
      DigitalSpecimenEvent entry, HashSet<String> uniqueMediaSet) {
    uniqueSet.add(entry);
    entry.digitalMediaEvents().forEach(mediaEvent -> uniqueMediaSet.add(
        getAccessUri(mediaEvent)));
  }

  // Pairs current specimens with current media in the DigitalSpecimenRecord

  private Map<String, DigitalSpecimenRecord> pairSpecimensWithMedia(
      List<DigitalSpecimenRecord> currentDigitalSpecimens,
      Map<String, DigitalMediaRecord> currentDigitalMedia) {

    var mediaIdMap = currentDigitalMedia.values().stream()
        .collect(Collectors.toMap(
            DigitalMediaRecord::id,
            Function.identity()
        ));
    return currentDigitalSpecimens.stream()
        .map(specimenRecord -> {
          var mediaIds = getCurrentDigitalMediaRecordsForSpecimen(
              specimenRecord.digitalSpecimenWrapper());
          var mediaUris = mediaIds.stream()
              .map(mediaIdMap::get)
              .map(DigitalMediaRecord::accessURI)
              .collect(Collectors.toSet());
          return new DigitalSpecimenRecord(
              specimenRecord.id(),
              specimenRecord.digitalSpecimenWrapper(),
              mediaUris
          );
        })
        .collect(Collectors.toMap(
            specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenId(),
            Function.identity()
        ));
  }


  /*
  Looks in the current version of the specimen and extracts the related media URIs
   */
  private static Set<String> getCurrentDigitalMediaRecordsForSpecimen(
      DigitalSpecimenWrapper currentSpecimenWrapper) {
    return currentSpecimenWrapper.attributes().getOdsHasEntityRelationships()
        .stream()
        .filter(er -> "hasDigitalMedia".equals(er.getDwcRelationshipOfResource()))
        .map(EntityRelationship::getOdsRelatedResourceURI)
        .map(URI::toString)
        .map(uri -> uri.replace("https://doi.org/", ""))
        .collect(Collectors.toSet());
  }

}
