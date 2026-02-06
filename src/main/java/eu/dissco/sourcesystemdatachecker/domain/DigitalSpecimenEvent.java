package eu.dissco.sourcesystemdatachecker.domain;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public record DigitalSpecimenEvent(
    Set<String> masList,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    List<DigitalMediaEvent> digitalMediaEvents,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem
) {

  public DigitalSpecimenEvent(Set<String> masList, DigitalSpecimenWrapper digitalSpecimenWrapper,
      List<DigitalMediaEvent> digitalMediaEvents, Boolean forceMasSchedule,
      Boolean isDataFromSourceSystem) {
    this.masList = masList;
    this.digitalSpecimenWrapper = digitalSpecimenWrapper;
    this.digitalMediaEvents = digitalMediaEvents;
    this.forceMasSchedule = forceMasSchedule;
    this.isDataFromSourceSystem = Objects.requireNonNullElse(isDataFromSourceSystem, Boolean.TRUE);
  }

}
