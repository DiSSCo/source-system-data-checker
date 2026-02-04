package eu.dissco.sourcesystemdatachecker.service;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.FilteredDigitalSpecimens;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
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
    var filteredMediaEvents = filterChangedAndNewMedia(filteredSpecimenEvents.unchangedEvents(),
        currentSpecimenRecords);
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
      return new FilteredDigitalSpecimens(Set.of(), new HashSet<>(specimenEventMap.values()));
    }
    var changedSpecimenEvents = specimenEventMap
        .entrySet()
        .stream()
        .filter(e -> !
            currentSpecimenRecords.containsKey(e.getKey()) ||
            currentSpecimenRecords.containsKey(e.getKey()) &&
                specimenIsChanged(e.getValue(), currentSpecimenRecords.get(e.getKey())))
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet());
    var unchangedSpecimens = specimenEventMap
        .values().stream()
        .filter(changedSpecimenEvents::contains).collect(Collectors.toSet());
    return new FilteredDigitalSpecimens(changedSpecimenEvents, unchangedSpecimens);
  }

  private static boolean specimenIsChanged(DigitalSpecimenEvent specimenEvent,
      DigitalSpecimenRecord currentSpecimenRecord) {
    return Objects.equals(specimenEvent.digitalSpecimenWrapper().originalAttributes(),
        currentSpecimenRecord.digitalSpecimenWrapper().originalAttributes()) &&
        specimenMediaEntityRelationshipsAreUnchanged(specimenEvent, currentSpecimenRecord);
  }

  private static boolean specimenMediaEntityRelationshipsAreUnchanged(
      DigitalSpecimenEvent specimenEvent, DigitalSpecimenRecord currentSpecimenRecord) {
    var incomingMedia = specimenEvent.digitalMediaEvents().stream()
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()).collect(
            Collectors.toSet());
    var existingMedia = currentSpecimenRecord.mediaRecords().stream().map(
        DigitalMediaRecord::accessURI).collect(
        Collectors.toSet());
    return incomingMedia.equals(existingMedia);
  }


  /*
    Takes incoming media events from UNCHANGED specimens and the corresponding records of the events that exist
    Returns a list of new media and changed media, filtering out unchanged specimens media
    This only filters out media that should be sent in the media-only queue.
    This list will be sent downstream to the ingestion process.
   */

  public Set<DigitalMediaEvent> filterChangedAndNewMedia(
      Set<DigitalSpecimenEvent> unchangedSpecimenEvents,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords) {
    if (unchangedSpecimenEvents.isEmpty()) {
      return Set.of();
    }
    var currentMediaRecords = currentSpecimenRecords.values().stream()
        .map(DigitalSpecimenRecord::mediaRecords)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(
            DigitalMediaRecord::accessURI,
            Function.identity()
        ));
    // We don't need to check if the media are new because the media ERs in "unchanged specimens" are unchanged
    return unchangedSpecimenEvents
        .stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .filter(mediaEvent -> mediaIsChanged(mediaEvent, currentMediaRecords.get(
            mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())))
        .collect(Collectors.toSet());
  }

  private static boolean mediaIsChanged(DigitalMediaEvent mediaEvent,
      DigitalMediaRecord mediaRecord) {
    return Objects.equals(mediaEvent.digitalMediaWrapper().originalAttributes(),
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

    var mediaUris = eventMap.values().stream().map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI())
        .collect(Collectors.toSet());
    var media = mediaRepository.getExistingDigitalMedia(mediaUris);
    return specimenRepository.getDigitalSpecimens(
            eventMap.keySet())
        .stream()
        .map(dbRecord -> {
          var event = eventMap.get(dbRecord.digitalSpecimenWrapper().physicalSpecimenId());
          return new DigitalSpecimenRecord(
              dbRecord.id(),
              dbRecord.midsLevel(),
              dbRecord.version(),
              dbRecord.created(),
              dbRecord.digitalSpecimenWrapper(),
              event.masList(),
              event.forceMasSchedule(),
              event.isDataFromSourceSystem(),
              getDigitalMediaRecordsForSpecimen(media, event));
        })
        .collect(
            toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenId(),
                Function.identity()));
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
          if (specimenIsNotPublished && checkIfMediaIsUnique(duplicateSpecimenEvent,
              uniqueMediaSet)) {
            addToUniqueSets(uniqueSet, duplicateSpecimenEvent, uniqueMediaSet);
            specimenIsNotPublished = false;
          } else {
            republishSpecimenEvent(duplicateSpecimenEvent);
          }
        }
      } else if (checkIfMediaIsUnique(entry.getValue().getFirst(), uniqueMediaSet)) {
        addToUniqueSets(uniqueSet, entry.getValue().getFirst(), uniqueMediaSet);
      } else {
        republishSpecimenEvent(entry.getValue().getFirst());
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

  private static boolean checkIfMediaIsUnique(DigitalSpecimenEvent entry,
      HashSet<String> uniqueMediaSet) {
    return entry.digitalMediaEvents().stream()
        .map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
        .noneMatch(uniqueMediaSet::contains);
  }

  private static void addToUniqueSets(LinkedHashSet<DigitalSpecimenEvent> uniqueSet,
      DigitalSpecimenEvent entry, HashSet<String> uniqueMediaSet) {
    uniqueSet.add(entry);
    uniqueMediaSet.addAll(entry.digitalMediaEvents().stream()
        .map(e -> e.digitalMediaWrapper().attributes().getAcAccessURI())
        .toList());
  }


  private static List<DigitalMediaRecord> getDigitalMediaRecordsForSpecimen(
      Map<String, DigitalMediaRecord> currentMediaRecords,
      DigitalSpecimenEvent specimenEvent) {
    var mediaUris = specimenEvent.digitalMediaEvents().stream()
        .map(mediaEvent -> mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI()).collect(
            Collectors.toSet());
    return mediaUris.stream().map(currentMediaRecords::get).toList();
  }


}
