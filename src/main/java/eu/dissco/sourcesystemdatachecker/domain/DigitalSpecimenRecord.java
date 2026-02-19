package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Set;

public record DigitalSpecimenRecord(
    String id,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<String> mediaUris
) {

}
