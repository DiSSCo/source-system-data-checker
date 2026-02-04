package eu.dissco.sourcesystemdatachecker.service;

import static java.util.stream.Collectors.toMap;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.properties.ApplicationProperties;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  private final ApplicationProperties applicationProperties;

  public void handleMessages(List<DigitalSpecimenEvent> events) {
    var specimenEventMap = events.stream().collect(Collectors.toMap(
        event -> event.digitalSpecimenWrapper().physicalSpecimenId(),
        Function.identity()
    ));
    var currentSpecimenRecords = getCurrentSpecimen(specimenEventMap);
    var filteredSpecimenEvents = filterChangedAndNewSpecimens(specimenEventMap, currentSpecimenRecords);

    var mediaEventMap = events.stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream).
        collect(Collectors.toMap(
            event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
            Function.identity()
        ));
    var currentMediaEvents = getCurrentMedia(mediaEventMap);
  }


  /*
    Takes incoming specimen events and the corresponding records of the events that exist
    Returns a list of new specimens and changed specimens, filtering out unchanged specimens
    This list will be sent downstream to the ingestion process.
    Note: currently does NOT filter out media that are unchanged from the specimen event.
    This will be done in a future PR.
   */

  public Pair<Set<DigitalSpecimenEvent>, Set<DigitalSpecimenEvent>> filterChangedAndNewSpecimens(
      Map<String, DigitalSpecimenEvent> specimenEventMap,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords) {
    if (currentSpecimenRecords.isEmpty()) {
      return Pair.of(Set.of(), new HashSet<>(specimenEventMap.values()));
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
    return Pair.of(changedSpecimenEvents, unchangedSpecimens);
  }

  private static boolean specimenIsChanged(DigitalSpecimenEvent specimenEvent,
      DigitalSpecimenRecord specimenRecord) {
    return Objects.equals(specimenEvent.digitalSpecimenWrapper().originalAttributes(),
        specimenRecord.digitalSpecimenWrapper().originalAttributes());
  }


  /*
    Takes incoming media events from UNCHANGED specimens and the corresponding records of the events that exist
    Returns a list of new media and changed media, filtering out unchanged specimens media
    This only filters out media that should be sent in the media-only queue.
    This list will be sent downstream to the ingestion process.
   */

  public List<DigitalMediaEvent> filterChangedAndNewMedia(
      Set<DigitalSpecimenEvent> unchangedSpecimenEvents, Set<DigitalSpecimenRecord> currentSpecimenRecords) {
    if (unchangedSpecimenEvents.isEmpty()) {
      return List.of();
    }
    var mediaMap = currentSpecimenRecords.stream()
        .map(DigitalSpecimenRecord::digitalMediaEvents)
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(
            event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
            Function.identity()
        ));
    var changedMedia = unchangedSpecimenEvents.stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .filter(Objects::nonNull)
        .filter(event -> )







  }

  private static boolean mediaIsChanged(DigitalMediaEvent mediaEvent,
      DigitalMediaRecord mediaRecord) {
    return Objects.equals(mediaEvent.digitalMediaWrapper().originalAttributes(),
        mediaEvent.digitalMediaWrapper().originalAttributes());
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
              List.of());
        })
        .collect( // todo add merge function
            toMap(
                specimenRecord -> specimenRecord.digitalSpecimenWrapper().physicalSpecimenId(),
                Function.identity()));
  }

  /*
    Takes incoming media events
    Checks repository for specimens with access URIs that match the events'
    Merges duplicate records
    Returns a map, where the key is the access URI
  */
  private Map<String, DigitalMediaRecord> getCurrentMedia(
      Map<String, DigitalMediaEvent> eventMap) {
    return mediaRepository.getExistingDigitalMedia(eventMap.keySet())
        .stream().filter(Objects::nonNull)
        .map(dbRecord ->
        {
          var event = eventMap.get(dbRecord.accessURI());
          return new DigitalMediaRecord(
              dbRecord.id(),
              dbRecord.accessURI(),
              dbRecord.attributes(),
              dbRecord.originalAttributes(),
              event.forceMasSchedule());
        })
        .collect(toMap( // todo verify
            DigitalMediaRecord::accessURI,
            Function.identity(),
            (uri1, uri2) -> {
              log.warn("Duplicate URIs found for digital media");
              return uri1;
            }
        ));
  }


}
