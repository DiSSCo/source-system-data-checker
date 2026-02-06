package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_DOI_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_URI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MEDIA_URI_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_2;
import static eu.dissco.sourcesystemdatachecker.TestUtils.SPECIMEN_DOI_1;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
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
  void testNewSpecimen() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
  }

  @Test
  void testUnchangedSpecimenNoMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent();
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testUnchangedSpecimenUnchangedMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).shouldHaveNoInteractions();
  }

  @Test
  void testChangedSpecimenNoMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Map.of());
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
  }

  @Test
  void testChangedSpecimenWithMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of(givenDigitalMediaEvent()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
  }


  @Test
  void testSpecimenWithNewMediaEr() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent(MEDIA_URI_2, false)));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
  }

  @Test
  void testSpecimenWithRemovedMediaEr() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord(), MEDIA_URI_2,
            givenDigitalMediaRecord(MEDIA_URI_2, MEDIA_DOI_1)));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord(SPECIMEN_DOI_1, PHYSICAL_ID_1, Set.of(
            MEDIA_URI_1, MEDIA_URI_2))));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
  }

  @Test
  void testUnchangedSpecimenChangedMedia() throws JsonProcessingException {
    // Given
    var mediaEvent = givenDigitalMediaEvent(MEDIA_URI_1, true);
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of(mediaEvent));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishMediaEvent(mediaEvent);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimen() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
    then(rabbitMqPublisherService).should().republishSpecimenEvent(event);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimenDuplicateMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
    then(rabbitMqPublisherService).should().republishSpecimenEvent(event);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimenDistinctMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent(MEDIA_DOI_2, false)));

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
    then(rabbitMqPublisherService).should().republishSpecimenEvent(event2);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
  }

  @Test
  void testTwoNewSpecimens() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of());

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishSpecimenEvent(any());
  }


  @Test
  void testOneNewSpecimenOneChanged() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of()); // exists, is changed
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of()); // New

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(List.of(givenDigitalSpecimenRecord()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishSpecimenEvent(any());
  }


  @Test
  void testOneNewSpecimenOneUnchanged() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false, List.of()); // exists, is changed
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of()); // New

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(List.of(givenDigitalSpecimenRecord()));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event2);
  }

  @Test
  void testTwoNewSpecimensNonUniqueMedia() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_2, false, List.of(givenDigitalMediaEvent()));

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishSpecimenEvent(any());
  }

  @Test
  void testNewDuplicateSpecimenRepublishFails() throws JsonProcessingException {
    // Given
    var event = givenDigitalSpecimenEvent();
    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());
    doThrow(JsonProcessingException.class).when(rabbitMqPublisherService).republishSpecimenEvent(event);

    // When
    service.handleMessages(List.of(event, event));

    // Then
    then(rabbitMqPublisherService).should().publishSpecimenEvent(event);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
  }


}
