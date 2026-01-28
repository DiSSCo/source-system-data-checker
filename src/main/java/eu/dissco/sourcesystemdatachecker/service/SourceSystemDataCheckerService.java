package eu.dissco.sourcesystemdatachecker.service;

import static java.util.stream.Collectors.toMap;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.exception.DisscoRepositoryException;
import eu.dissco.sourcesystemdatachecker.properties.ApplicationProperties;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private final ApplicationProperties applicationProperties;

  public void handleMessages(List<DigitalSpecimenEvent> events) {
    var specimenEventMap = events.stream().collect(Collectors.toMap(
        event -> event.digitalSpecimenWrapper().physicalSpecimenId(),
        Function.identity()
    ));
    var currentSpecimenEvents = getCurrentSpecimen(specimenEventMap);

    var mediaEventMap = events.stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream).
        collect(Collectors.toMap(
            event -> event.digitalMediaWrapper().attributes().getAcAccessURI(),
            Function.identity()
        ));
    var currentMediaEvents = getCurrentMedia(mediaEventMap);
  }

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
