package eu.dissco.sourcesystemdatachecker.service;

import static java.util.stream.Collectors.toMap;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigitalSpecimens;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigtialMedia;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.net.URI;
import java.util.Collection;
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
    var filteredSpecimenEvents = filterChangedAndNewSpecimens(specimenEventMap,
        currentSpecimensWithMediaUris);
    log.info("{} specimens are new or changed; {} specimens are unchanged",
        filteredSpecimenEvents.newOrChangedSpecimens().size(),
        filteredSpecimenEvents.unchangedSpecimens().size());
    var filteredMediaEvents = filterChangedAndNewMedia(
        filteredSpecimenEvents.unchangedSpecimens().values(),
        currentMediaRecords);
    log.info("{} media are changed and belong to an unchanged specimen",
        filteredMediaEvents.newOrChangedMedia());
    updateLastCheckedSpecimens(filteredSpecimenEvents.unchangedSpecimens().keySet());
    updateLastCheckedMedia(filteredMediaEvents.unchangedMedia());
    log.info("Successfully updated lastChecked for {} specimens and {} media",
        filteredSpecimenEvents.unchangedSpecimens().size(),
        filteredMediaEvents.unchangedMedia().size());
    publishChangedAndNewSpecimens(filteredSpecimenEvents.newOrChangedSpecimens());
    publishedChangedMedia(filteredMediaEvents.newOrChangedMedia());
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

  /*
    Takes incoming specimen events and the corresponding records of the events that exist
    Returns a list of new specimens and changed specimens, filtering out unchanged specimens
    This list will be sent downstream to the ingestion process.
    Note: currently does NOT filter out media that are unchanged from the specimen event.
    This will be done in a future PR.
   */

  public FilteredDigitalSpecimens filterChangedAndNewSpecimens(
      Map<String, DigitalSpecimenEvent> specimenEventMap,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords) {
    if (currentSpecimenRecords.isEmpty()) {
      return new FilteredDigitalSpecimens(new HashSet<>(specimenEventMap.values()), Map.of());
    }
    var changedSpecimenEvents = specimenEventMap
        .entrySet()
        .stream()
        .filter(event ->
            !currentSpecimenRecords.containsKey(event.getKey()) ||
                specimenIsChanged(event.getValue(), currentSpecimenRecords.get(event.getKey())))
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet());
    var unchangedSpecimens = specimenEventMap
        .values().stream()
        .filter(specimenEvent -> !changedSpecimenEvents.contains(specimenEvent))
        .collect(Collectors.toMap(
            event -> currentSpecimenRecords.get(event.digitalSpecimenWrapper().physicalSpecimenId())
                .id(),
            Function.identity()
        ));
    return new FilteredDigitalSpecimens(changedSpecimenEvents, unchangedSpecimens);
  }

  private static boolean specimenIsChanged(DigitalSpecimenEvent specimenEvent,
      DigitalSpecimenRecord currentSpecimenRecord) {
    return !Objects.equals(specimenEvent.digitalSpecimenWrapper().originalAttributes(),
        currentSpecimenRecord.digitalSpecimenWrapper().originalAttributes()) ||
        specimenMediaEntityRelationshipsAreChanged(specimenEvent, currentSpecimenRecord);
  }

  private static boolean specimenMediaEntityRelationshipsAreChanged(
      DigitalSpecimenEvent specimenEvent, DigitalSpecimenRecord currentSpecimenRecord) {
    var incomingMedia = specimenEvent.digitalMediaEvents().stream()
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()).collect(
            Collectors.toSet());
    return !incomingMedia.equals(currentSpecimenRecord.mediaUris());
  }

  /*
    Takes incoming media events from UNCHANGED specimens and the corresponding records of the events that exist
    Returns a list of new media and changed media, filtering out unchanged specimens media
    This only filters out media that should be sent in the media-only queue.
    This list will be sent downstream to the ingestion process.
   */

  public FilteredDigtialMedia filterChangedAndNewMedia(
      Collection<DigitalSpecimenEvent> unchangedSpecimenEvents,
      Map<String, DigitalMediaRecord> currentMediaRecords) {
    // No unchanged specimens, so all media will be published with the specimen
    if (unchangedSpecimenEvents.isEmpty()) {
      return new FilteredDigtialMedia(Set.of(), Set.of());
    }

    var changedMediaWithUnchangedSpecimen = unchangedSpecimenEvents
        .stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .filter(mediaEvent ->
            mediaIsChanged(mediaEvent, currentMediaRecords.get(
                mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()))
                || !currentMediaRecords.containsKey(
                mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()))
        .collect(Collectors.toMap(
            event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
            Function.identity()
        ));
    var unchangedMedia = currentMediaRecords.entrySet().stream()
        .filter(e -> !changedMediaWithUnchangedSpecimen.containsKey(e.getKey()))
        .map(Entry::getValue)
        .collect(Collectors.toSet());
    return new FilteredDigtialMedia(new HashSet<>(changedMediaWithUnchangedSpecimen.values()),
        unchangedMedia);
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
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())
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
        mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()));
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
