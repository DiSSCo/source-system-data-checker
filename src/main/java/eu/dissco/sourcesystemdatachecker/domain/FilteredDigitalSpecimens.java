package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Map;
import java.util.Set;

public record FilteredDigitalSpecimens(
    Set<DigitalSpecimenEvent> newOrChangedSpecimens,
    Map<String, DigitalSpecimenEvent> unchangedSpecimens
) {

}
