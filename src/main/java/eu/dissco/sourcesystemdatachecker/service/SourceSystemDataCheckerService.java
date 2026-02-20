package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.service.ServiceUtils.DOI_PROXY;
import static eu.dissco.sourcesystemdatachecker.service.ServiceUtils.getAccessUri;

import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.media.FilteredDigtialMedia;
import eu.dissco.sourcesystemdatachecker.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.domain.specimen.FilteredDigitalSpecimens;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
  private final MasSchedulerService masSchedulerService;

  public void handleMessages(Set<DigitalSpecimenEvent> events) {
    log.info("Received {} unique events", events.size());
    var specimenEventMap = events.stream().collect(Collectors.toMap(
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
    scheduleMasForSpecimen(filteredSpecimenEvents);
    scheduleMasForMedia(filteredMediaEvents, events);
  }

  private void scheduleMasForSpecimen(FilteredDigitalSpecimens filteredDigitalSpecimens) {
    if (!filteredDigitalSpecimens.unchangedSpecimens().isEmpty()) {
      masSchedulerService.scheduleMasForSpecimen(filteredDigitalSpecimens.unchangedSpecimens());
    }
  }

  private void scheduleMasForMedia(FilteredDigtialMedia filteredDigtialMedia,
      Set<DigitalSpecimenEvent> specimenEvents) {
    if (filteredDigtialMedia.unchangedMedia().isEmpty()) {
      return;
    }
    masSchedulerService.scheduleMasForMedia(filteredDigtialMedia, specimenEvents);
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
    An unchanged specimen is one whose original data is unchanged AND its media ERs are unchanged (none added, none removed)
    This list of changed will be sent downstream to the ingestion process
   */
  public FilteredDigitalSpecimens filterChangedAndNewSpecimens(
      Map<String, DigitalSpecimenEvent> specimenEventMap,
      Map<String, DigitalSpecimenRecord> currentSpecimenRecords) {
    if (currentSpecimenRecords.isEmpty()) {
      return new FilteredDigitalSpecimens(new HashSet<>(specimenEventMap.values()), Map.of());
    }
    var changedSpecimens = new HashSet<DigitalSpecimenEvent>();
    var unchangedSpecimens = new HashMap<String, DigitalSpecimenEvent>();
    specimenEventMap.forEach((key, value) -> {
      var currentSpecimen = currentSpecimenRecords.get(key);
      if (currentSpecimen == null || specimenIsChanged(value, currentSpecimenRecords.get(key))) {
        changedSpecimens.add(value);
      } else {
        unchangedSpecimens.put(currentSpecimen.id(), value);
      }
    });
    return new FilteredDigitalSpecimens(changedSpecimens, unchangedSpecimens);
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
        .map(ServiceUtils::getAccessUri).collect(
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
    var changedMediaWithUnchangedSpecimens = new HashSet<DigitalMediaEvent>();
    var unchangedMedia = new HashSet<DigitalMediaRecord>();
    unchangedSpecimenEvents
        .stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .forEach(mediaEvent -> {
          var currentMedia = currentMediaRecords.get(getAccessUri(mediaEvent));
          if (currentMedia == null ||
              mediaIsChanged(mediaEvent, currentMediaRecords.get(getAccessUri(mediaEvent)))) {
            changedMediaWithUnchangedSpecimens.add(mediaEvent);
          } else {
            unchangedMedia.add(currentMedia);
          }
        });
    return new FilteredDigtialMedia(changedMediaWithUnchangedSpecimens, unchangedMedia);
  }

  private static boolean mediaIsChanged(DigitalMediaEvent mediaEvent,
      DigitalMediaRecord mediaRecord) {
    return !Objects.equals(mediaEvent.digitalMediaWrapper().originalAttributes(),
        mediaRecord.originalAttributes());
  }

  private List<DigitalSpecimenRecord> getCurrentSpecimen(
      Map<String, DigitalSpecimenEvent> eventMap) {
    return specimenRepository.getDigitalSpecimens(eventMap.keySet());
  }

  private Map<String, DigitalMediaRecord> getCurrentMedia(
      Map<String, DigitalSpecimenEvent> specimenEventMap) {

    var incomingMediaUris = specimenEventMap.values().stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        .map(ServiceUtils::getAccessUri)
        .collect(Collectors.toSet());
    return mediaRepository.getExistingDigitalMedia(incomingMediaUris);
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
        .map(uri -> uri.replace(DOI_PROXY, ""))
        .collect(Collectors.toSet());
  }
}
