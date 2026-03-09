package eu.dissco.sourcesystemdatachecker.domain.specimen;

import java.util.Map;

public record DigitalSpecimenRecord(
    String id,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Map<String, String> mediaUriMap
) {

}
