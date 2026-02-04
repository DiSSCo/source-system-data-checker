package eu.dissco.sourcesystemdatachecker.domain;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    Instant created,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<String> masIds,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem,
    List<DigitalMediaRecord> mediaRecords
) {

}
