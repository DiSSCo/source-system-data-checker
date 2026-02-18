package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Set;

public record DigitalSpecimenEventWithFilteredMedia(
    Set<String> masList,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<DigitalMediaRecord> unchangedMedia,
    Set<DigitalMediaEvent> changedMedia,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem
) {

}
