package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Set;

public record DigitalMediaEvent(
    Set<String> masList,
    DigitalMediaWrapper digitalMediaWrapper,
    Boolean forceMasSchedule
) {

}
