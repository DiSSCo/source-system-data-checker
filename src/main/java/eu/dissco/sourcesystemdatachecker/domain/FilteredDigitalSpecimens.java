package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Set;

public record FilteredDigitalSpecimens(
    Set<DigitalSpecimenEvent> newOrChangedEvents,
    Set<DigitalSpecimenEvent> unchangedEvents
) {

}
