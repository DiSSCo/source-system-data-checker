package eu.dissco.sourcesystemdatachecker.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record DigitalSpecimenEvent(
    Set<String> masList,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    @JsonProperty("digitalMediaEvents")
    List<DigitalMediaEvent> digitalMediaEvents,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem
) {

  public DigitalSpecimenEvent(Set<String> masList, DigitalSpecimenWrapper digitalSpecimenWrapper,
      List<DigitalMediaEvent> digitalMediaEvents, Boolean forceMasSchedule,
      Boolean isDataFromSourceSystem) {
    this.masList = masList;
    this.digitalSpecimenWrapper = digitalSpecimenWrapper;
    this.digitalMediaEvents = Objects.requireNonNullElse(digitalMediaEvents, List.of());
    this.forceMasSchedule = forceMasSchedule;
    this.isDataFromSourceSystem = Objects.requireNonNullElse(isDataFromSourceSystem, Boolean.TRUE);
  }

}
