package eu.dissco.sourcesystemdatachecker.domain.media;

import eu.dissco.sourcesystemdatachecker.schema.DigitalMedia;
import tools.jackson.databind.JsonNode;

public record DigitalMediaRecord(
    String id,
    String accessURI,
    DigitalMedia attributes,
    JsonNode originalAttributes
) {

}
