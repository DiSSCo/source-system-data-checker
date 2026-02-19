package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_URI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_URI_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.SPECIMEN_DOI;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEventWithMedia;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenRecordWithMedia;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SourceSystemDataCheckerServiceTest {

  @Mock
  private SpecimenRepository specimenRepository;
  @Mock
  private MediaRepository mediaRepository;
  @Mock
  private RabbitMqPublisherService rabbitMqPublisherService;

  private SourceSystemDataCheckerService service;

  @BeforeEach
  void init() {
    service = new SourceSystemDataCheckerService(specimenRepository,
        mediaRepository, rabbitMqPublisherService);
  }

  @Test
  void testNewSpecimen() {
    // Given
    var event = givenDigitalSpecimenEvent();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
  }

  @Test
  void testUnchangedSpecimenNoMedia() {
    // Given
    var event = givenDigitalSpecimenEvent();
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(specimenRepository).should().updateLastChecked(Set.of(SPECIMEN_DOI));
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testUnchangedSpecimenUnchangedMedia() {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
    then(specimenRepository).should().updateLastChecked(Set.of(SPECIMEN_DOI));
    then(mediaRepository).should().updateLastChecked(Set.of(MEDIA_DOI_1));
  }

  @Test
  void testChangedSpecimenNoMedia() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Map.of());
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testChangedSpecimenWithMedia() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of(givenDigitalMediaEvent()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }


  @Test
  void testSpecimenWithNewMediaEr() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent(MEDIA_URI_2, false)));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testSpecimenWithRemovedMediaEr() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord(), MEDIA_URI_2,
            givenDigitalMediaRecord(MEDIA_DOI_2, MEDIA_URI_2)));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord(SPECIMEN_DOI, PHYSICAL_ID_1, Map.of(
            MEDIA_URI_1, MEDIA_DOI_1, MEDIA_URI_2, MEDIA_DOI_2))));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testUnchangedSpecimenChangedMedia() {
    // Given
    var mediaEvent = givenDigitalMediaEvent(MEDIA_URI_1, true);
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of(mediaEvent));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(Set.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishMediaEvent(mediaEvent);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should().updateLastChecked(Set.of(SPECIMEN_DOI));
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }


  @Test
  void testTwoNewSpecimens() {
    // Given
    var event = givenDigitalSpecimenEvent();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of());

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(Set.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishNameUsageEvent(any());
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }


  @Test
  void testOneNewSpecimenOneChanged() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of()); // exists, is changed
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of()); // New

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(
        List.of(givenDigitalSpecimenRecord()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(Set.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishNameUsageEvent(any());
  }


  @Test
  void testOneNewSpecimenOneUnchanged() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of()); // exists, is changed
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of()); // New

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(
        List.of(givenDigitalSpecimenRecord()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(Set.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event2);
  }

  @Test
  void testTwoNewSpecimensNonUniqueMedia() {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of(givenDigitalMediaEvent()));

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(Set.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishNameUsageEvent(any());
  }

}
