package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.service.ServiceUtils.DOI_PROXY;
import static eu.dissco.sourcesystemdatachecker.service.ServiceUtils.getAccessUri;

import eu.dissco.sourcesystemdatachecker.domain.mas.MasJobRequest;
import eu.dissco.sourcesystemdatachecker.domain.mas.MjrTargetType;
import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaRecord;
import eu.dissco.sourcesystemdatachecker.domain.media.FilteredDigtialMedia;
import eu.dissco.sourcesystemdatachecker.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.properties.ApplicationProperties;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Slf4j
@Service
public class MasSchedulerService {

  private final RabbitMqPublisherService publisherService;
  private final ApplicationProperties applicationProperties;

  // Map key is specimen DOI
  public void scheduleMasForSpecimen(Map<String, DigitalSpecimenEvent> unchangedSpecimenEventMap) {
    var recordsToSchedule = getMasJobRequestsForSpecimens(unchangedSpecimenEventMap);
    publishMas(recordsToSchedule);
    log.debug("Scheduled {} forced MAS Jobs on unchanged specimens", recordsToSchedule.size());
  }

  public void scheduleMasForMedia(FilteredDigtialMedia filteredDigtialMedia,
      Set<DigitalSpecimenEvent> specimenEvents) {
    var recordsToSchedule = getMasJobRequestsForMedia(filteredDigtialMedia, specimenEvents);
    publishMas(recordsToSchedule);
    log.debug("Scheduled {} forced MAS Jobs on unchanged media", recordsToSchedule.size());
  }

  private void publishMas(Set<MasJobRequest> masJobRequests) {
    masJobRequests.forEach(publisherService::publishMasJobRequest);
  }

  private Set<MasJobRequest> getMasJobRequestsForSpecimens(
      Map<String, DigitalSpecimenEvent> unchangedSpecimenEvents) {
    return unchangedSpecimenEvents.entrySet().stream()
        .filter(entry -> entry.getValue().forceMasSchedule())
        .filter(entry -> !entry.getValue().masList().isEmpty())
        .map(entry -> entry.getValue().masList().stream()
            .map(masId -> buildMasJobRequest(masId, entry.getKey(), MjrTargetType.DIGITAL_SPECIMEN))
            .collect(Collectors.toSet()))
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private Set<MasJobRequest> getMasJobRequestsForMedia(
      FilteredDigtialMedia filteredDigtialMedia, Set<DigitalSpecimenEvent> specimenEvents) {
    var unchangedMediaRecordMap = filteredDigtialMedia.unchangedMedia().stream().collect(Collectors.toMap(
        DigitalMediaRecord::accessURI,
        Function.identity()
    ));
    // Only schedule MAS jobs on existing records
    return specimenEvents.stream()
        .map(DigitalSpecimenEvent::digitalMediaEvents)
        .flatMap(Collection::stream)
        // Get unchanged media events from the specimen events
        .filter(mediaEvent -> unchangedMediaRecordMap.containsKey(getAccessUri(mediaEvent)))
        // Filter out media events that do not need MAS scheduling
        .filter(DigitalMediaEvent::forceMasSchedule)
        // Filter out media events that do not have any MASs requested
        .filter(entry -> !entry.masList().isEmpty())
        // Transform Media Records into MAS Job Requests
        .map(entry -> entry.masList().stream()
            .map(masId -> buildMasJobRequest(masId, unchangedMediaRecordMap.get(getAccessUri(entry)).id(),
                MjrTargetType.DIGITAL_MEDIA))
            .collect(Collectors.toSet()))
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private MasJobRequest buildMasJobRequest(String masId, String targetId,
      MjrTargetType targetType) {
    return new MasJobRequest(
        masId,
        DOI_PROXY + targetId,
        false,
        applicationProperties.getPid(),
        targetType
    );
  }

}
