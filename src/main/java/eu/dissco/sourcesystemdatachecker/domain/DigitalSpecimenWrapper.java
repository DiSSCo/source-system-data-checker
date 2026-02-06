package eu.dissco.sourcesystemdatachecker.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;

public record DigitalSpecimenWrapper(
    @JsonProperty("ods:normalisedPhysicalSpecimenID")
    String physicalSpecimenId,
    @JsonProperty("ods:type")
    String type,
    @JsonProperty("ods:attributes")
    DigitalSpecimen attributes,
    @JsonProperty("ods:originalAttributes")
    JsonNode originalAttributes
) {

}
