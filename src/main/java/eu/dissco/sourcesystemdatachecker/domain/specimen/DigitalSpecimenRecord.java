package eu.dissco.sourcesystemdatachecker.domain.specimen;

import java.util.Set;

public record DigitalSpecimenRecord(
    String id,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<String> mediaUris
) {

}
