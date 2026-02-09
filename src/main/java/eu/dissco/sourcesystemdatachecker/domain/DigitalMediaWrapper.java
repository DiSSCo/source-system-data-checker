package eu.dissco.sourcesystemdatachecker.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import eu.dissco.sourcesystemdatachecker.schema.DigitalMedia;
import tools.jackson.databind.JsonNode;

public record DigitalMediaWrapper(
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:attributes")
    DigitalMedia attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes) {

}
