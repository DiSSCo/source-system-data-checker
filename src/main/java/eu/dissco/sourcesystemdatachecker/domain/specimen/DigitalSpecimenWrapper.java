package eu.dissco.sourcesystemdatachecker.domain.specimen;

import com.fasterxml.jackson.annotation.JsonProperty;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import tools.jackson.databind.JsonNode;

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
