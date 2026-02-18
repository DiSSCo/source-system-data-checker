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
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEventWithMediaEr;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenRecordWithMedia;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenOriginalAttributes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.repository.MediaRepository;
import eu.dissco.sourcesystemdatachecker.repository.SpecimenRepository;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
  @Captor
  private ArgumentCaptor<DigitalSpecimenEvent> captor;

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
    service.handleMessages(List.of(event));

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
    service.handleMessages(List.of(event));

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
    service.handleMessages(List.of(event));

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
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testChangedSpecimenWithUnchangedMedia() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of(givenDigitalMediaEvent()));
    var expected = givenDigitalSpecimenEventWithMediaEr(PHYSICAL_ID_1, true, List.of(),
        List.of(MEDIA_DOI_1));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(expected);
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).should().updateLastChecked(Set.of(MEDIA_DOI_1));
  }

  @Test
  void testSpecimenWithNewMedia() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent(MEDIA_URI_2, false)));
    var expected = givenDigitalSpecimenEventWithMediaEr(
        PHYSICAL_ID_1,
        false,
        List.of(givenDigitalMediaEvent(MEDIA_URI_2, false)),
        List.of(MEDIA_DOI_1)
    );
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord()));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecordWithMedia()));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(expected);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).should().updateLastChecked(Set.of(MEDIA_DOI_1));
  }

  @Test
  void testChangedSpecimenWithOneChangedMedia() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true,
        List.of(givenDigitalMediaEvent(MEDIA_URI_1, true),
            givenDigitalMediaEvent(MEDIA_URI_2, false)));
    var expected = givenDigitalSpecimenEventWithMediaEr(
        PHYSICAL_ID_1, true,
        List.of(givenDigitalMediaEvent(MEDIA_URI_1, true)),
        List.of(MEDIA_DOI_1, MEDIA_DOI_2));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord(), MEDIA_URI_2,
            givenDigitalMediaRecord(MEDIA_DOI_2, MEDIA_URI_2)));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord(SPECIMEN_DOI, PHYSICAL_ID_1,
            Map.of(MEDIA_URI_1, MEDIA_DOI_1, MEDIA_URI_2, MEDIA_DOI_2))));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(captor.capture());
    var result = captor.getValue();
    assertThat(new HashSet<>(result.digitalMediaEvents())).isEqualTo(
        new HashSet<>(expected.digitalMediaEvents()));
    assertThat(
        new HashSet<>(result.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships()))
        .isEqualTo(new HashSet<>(
            expected.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships()));
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).should().updateLastChecked(Set.of(MEDIA_DOI_2));
  }

  @Test
  void testSpecimenWithOneUnchangedMediaOneRemovedMediaEr() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent()));
    var expected = givenDigitalSpecimenEventWithMediaEr(PHYSICAL_ID_1, false,
        List.of(), List.of(MEDIA_DOI_1));
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of(MEDIA_URI_1, givenDigitalMediaRecord(), MEDIA_URI_2,
            givenDigitalMediaRecord(MEDIA_DOI_2, MEDIA_URI_2)));
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(givenDigitalSpecimenRecord(SPECIMEN_DOI, PHYSICAL_ID_1, Map.of(
            MEDIA_URI_1, MEDIA_DOI_1, MEDIA_URI_2, MEDIA_DOI_2))));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(expected);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).should().updateLastChecked(Set.of(MEDIA_DOI_1));
  }

  @Test
  void testSpecimenHasPreviousEr() {
    // Given
    var event = givenDigitalSpecimenEvent(PHYSICAL_ID_1, true, List.of());
    var entityRelationship = new EntityRelationship()
        .withType("ods:hasEntityRelationship")
        .withDwcRelationshipOfResource("Some-relation")
        .withDwcRelatedResourceID("https://related-id.com")
        .withOdsRelatedResourceURI(URI.create("https://related-id.com"));
    var currentSpecimenRecord =
        new DigitalSpecimenRecord(
            SPECIMEN_DOI,
            new DigitalSpecimenWrapper(
                PHYSICAL_ID_1,
                "ods:DigitalSpecimen",
                new DigitalSpecimen()
                    .withOdsHasEntityRelationships(List.of(entityRelationship)),
                givenOriginalAttributes(false)
            ),
            Set.of()
        );
    var expected = new DigitalSpecimenEvent(
        event.masList(),
        new DigitalSpecimenWrapper(
            PHYSICAL_ID_1,
            "ods:DigitalSpecimen",
            new DigitalSpecimen()
                .withOdsHasEntityRelationships(List.of(entityRelationship)),
            givenOriginalAttributes(true)
        ),
        event.digitalMediaEvents(),
        event.forceMasSchedule(),
        event.isDataFromSourceSystem()
    );
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(
        Map.of());
    given(specimenRepository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1))).willReturn(
        List.of(currentSpecimenRecord));

    // When
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(expected);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
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
    service.handleMessages(List.of(event));

    // Then
    then(rabbitMqPublisherService).should().publishMediaEvent(mediaEvent);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).should().updateLastChecked(Set.of(SPECIMEN_DOI));
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimen() {
    // Given
    var event = givenDigitalSpecimenEvent();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(rabbitMqPublisherService).should().republishEvent(event);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimenDuplicateMedia() {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(rabbitMqPublisherService).should().republishEvent(event);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
    then(mediaRepository).shouldHaveNoMoreInteractions();
  }

  @Test
  void testNewDuplicateSpecimenDistinctMedia() {
    // Given
    var event = givenDigitalSpecimenEventWithMedia();
    var event2 = givenDigitalSpecimenEvent(PHYSICAL_ID_1, false,
        List.of(givenDigitalMediaEvent(MEDIA_DOI_2, false)));

    given(specimenRepository.getDigitalSpecimens(anySet())).willReturn(Collections.emptyList());
    given(mediaRepository.getExistingDigitalMedia(anySet())).willReturn(Collections.emptyMap());

    // When
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should().publishNameUsageEvent(event);
    then(rabbitMqPublisherService).should().republishEvent(event2);
    then(rabbitMqPublisherService).shouldHaveNoMoreInteractions();
    then(specimenRepository).shouldHaveNoMoreInteractions();
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
    service.handleMessages(List.of(event, event2));

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
    service.handleMessages(List.of(event, event2));

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
    service.handleMessages(List.of(event, event2));

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
    service.handleMessages(List.of(event, event2));

    // Then
    then(rabbitMqPublisherService).should(times(2)).publishNameUsageEvent(any());
  }

}
