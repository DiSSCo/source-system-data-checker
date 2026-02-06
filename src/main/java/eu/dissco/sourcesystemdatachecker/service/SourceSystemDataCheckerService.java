package eu.dissco.sourcesystemdatachecker.service;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigitalSpecimens;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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

  public void handleMessages(List<DigitalSpecimenEvent> events) throws JsonProcessingException {
    var uniqueEvents = removeDuplicatesInBatch(events);
    var specimenEventMap = uniqueEvents.stream().collect(Collectors.toMap(
        event -> event.digitalSpecimenWrapper().physicalSpecimenId(),
        Function.identity()
    ));
    var currentSpecimenRecords = getCurrentSpecimen(specimenEventMap);
    var filteredSpecimenEvents = filterChangedAndNewSpecimens(specimenEventMap,
        currentSpecimenRecords);
    var currentMediaRecords = getCurrentMedia(specimenEventMap);
    var filteredMediaEvents = filterChangedAndNewMedia(filteredSpecimenEvents.unchangedEvents(),
        currentMediaRecords);
    publishChangedAndNewSpecimens(filteredSpecimenEvents.newOrChangedEvents());
    publishedChangedMedia(filteredMediaEvents);
  }

  private void publishChangedAndNewSpecimens(Set<DigitalSpecimenEvent> digitalSpecimenEvents)
      throws JsonProcessingException {
    for (var digitalSpecimenEvent : digitalSpecimenEvents) {
      rabbitMqPublisherService.publishSpecimenEvent(digitalSpecimenEvent);
    }
  }

  private void publishedChangedMedia(Set<DigitalMediaEvent> digitalMediaEvents)
      throws JsonProcessingException {
    for (var digitalMediaEvent : digitalMediaEvents) {
      rabbitMqPublisherService.publishMediaEvent(digitalMediaEvent);
    }
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
      return new FilteredDigitalSpecimens(new HashSet<>(specimenEventMap.values()), Set.of());
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
        .collect(Collectors.toSet());
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

  public Set<DigitalMediaEvent> filterChangedAndNewMedia(
      Set<DigitalSpecimenEvent> unchangedSpecimenEvents,
      Map<String, DigitalMediaRecord> currentMediaRecords) {
    if (unchangedSpecimenEvents.isEmpty()) {
      return Set.of();
    }

    // We check if media are new in case a specimen has an ER to a media that hasn't been published
    return unchangedSpecimenEvents
        .stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .filter(mediaEvent ->
            mediaIsChanged(mediaEvent, currentMediaRecords.get(
                mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()))
                || !currentMediaRecords.containsKey(
                mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()))
        .collect(Collectors.toSet());
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
  private Map<String, DigitalSpecimenRecord> getCurrentSpecimen(
      Map<String, DigitalSpecimenEvent> eventMap) {
    return specimenRepository.getDigitalSpecimens(
            eventMap.keySet())
        .stream()
        .map(dbRecord ->
            new DigitalSpecimenRecord(
                dbRecord.id(),
                dbRecord.digitalSpecimenWrapper(),
                getCurrentDigitalMediaRecordsForSpecimen(dbRecord.digitalSpecimenWrapper())))
        .collect(
            toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenId(),
                Function.identity()));
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
    try {
      rabbitMqPublisherService.republishSpecimenEvent(event);
    } catch (JsonProcessingException e) {
      log.error("Fatal exception, unable to republish specimen message due to invalid json", e);
    }
  }

  private static void addToUniqueSets(LinkedHashSet<DigitalSpecimenEvent> uniqueSet,
      DigitalSpecimenEvent entry, HashSet<String> uniqueMediaSet) {
    uniqueSet.add(entry);
    uniqueMediaSet.addAll(entry.digitalMediaEvents().stream()
        .map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
        .toList());
  }

  /*
  Looks in the current version of the specimen and extracts the related media URIs
   */
  private static Set<String> getCurrentDigitalMediaRecordsForSpecimen(
      DigitalSpecimenWrapper currentSpecimenWrapper) {
    return currentSpecimenWrapper.attributes().getOdsHasEntityRelationships()
        .stream()
        .filter(er -> "hasMedia".equals(er.getDwcRelationshipOfResource()))
        .map(EntityRelationship::getDwcRelatedResourceID)
        .collect(Collectors.toSet());
  }

}
