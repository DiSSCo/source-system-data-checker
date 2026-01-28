package eu.dissco.sourcesystemdatachecker.domain;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.sourcesystemdatachecker.schema.DigitalMedia;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    DigitalMedia attributes,
    JsonNode originalAttributes,
    Boolean forceMasSchedule
) {

}
