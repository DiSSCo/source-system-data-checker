package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.TestUtils.APP_PID;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MAS_ID;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.SPECIMEN_DOI;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaEventWithMasSchedule;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEventWithMasSchedule;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEventWithMedia;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenMasJobRequest;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.sourcesystemdatachecker.domain.mas.MjrTargetType;
import eu.dissco.sourcesystemdatachecker.domain.media.FilteredDigtialMedia;
import eu.dissco.sourcesystemdatachecker.properties.ApplicationProperties;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MasSchedulerServiceTest {

  @Mock
  private RabbitMqPublisherService publisherService;
  @Mock
  private ApplicationProperties applicationProperties;
  MasSchedulerService masSchedulerService;

  @BeforeEach
  void setup() {
    masSchedulerService = new MasSchedulerService(publisherService,
        applicationProperties);
  }

  @Test
  void testScheduleMasForSpecimen() {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_PID);
    var eventMap = Map.of(SPECIMEN_DOI, givenDigitalSpecimenEventWithMasSchedule(Set.of(MAS_ID)
    ));

    // When
    masSchedulerService.scheduleMasForSpecimen(eventMap);

    // Then
    then(publisherService).should()
        .publishMasJobRequest(givenMasJobRequest(SPECIMEN_DOI, MjrTargetType.DIGITAL_SPECIMEN));
  }

  @Test
  void testScheduleMasForSpecimenNotForced() {
    // Given

    // When
    masSchedulerService.scheduleMasForSpecimen(Map.of(SPECIMEN_DOI, givenDigitalSpecimenEvent()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasForSpecimenNoMas() {
    // Given
    var event = givenDigitalSpecimenEventWithMasSchedule(Set.of());

    // When
    masSchedulerService.scheduleMasForSpecimen(Map.of(SPECIMEN_DOI, event));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasForMedia() {
    // Given
    given(applicationProperties.getPid()).willReturn(APP_PID);
    var filteredMedia = new FilteredDigtialMedia(Set.of(), Set.of(givenDigitalMediaRecord()));
    var mediaEvent = givenDigitalMediaEventWithMasSchedule(Set.of(MAS_ID));
    var expected = givenMasJobRequest(MEDIA_DOI_1, MjrTargetType.DIGITAL_MEDIA);

    // When
    masSchedulerService.scheduleMasForMedia(filteredMedia,
        Set.of(givenDigitalSpecimenEventWithMasSchedule(Set.of(MAS_ID), List.of(mediaEvent))));

    // Then
    then(publisherService).should().publishMasJobRequest(expected);
  }

  @Test
  void testScheduleMasForMediaNoForceSchedule() {
    // Given
    var filteredMedia = new FilteredDigtialMedia(Set.of(), Set.of(givenDigitalMediaRecord()));

    // When
    masSchedulerService.scheduleMasForMedia(filteredMedia,
        Set.of(givenDigitalSpecimenEventWithMedia()));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

  @Test
  void testScheduleMasForMediaNoMas() {
    // Given
    var filteredMedia = new FilteredDigtialMedia(Set.of(), Set.of(givenDigitalMediaRecord()));
    var mediaEvent = givenDigitalMediaEventWithMasSchedule(Set.of());

    // When
    masSchedulerService.scheduleMasForMedia(filteredMedia,
        Set.of(givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of(mediaEvent))));

    // Then
    then(publisherService).shouldHaveNoInteractions();
  }

}
