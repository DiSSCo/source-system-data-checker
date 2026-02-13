package eu.dissco.sourcesystemdatachecker.repository;

import static eu.dissco.sourcesystemdatachecker.database.jooq.Tables.DIGITAL_SPECIMEN;

import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
@RequiredArgsConstructor
@Slf4j
public class SpecimenRepository {

  private final DSLContext context;
  private final JsonMapper mapper;

  public List<DigitalSpecimenRecord> getDigitalSpecimens(Set<String> specimenList) {
    return context.select(DIGITAL_SPECIMEN.asterisk())
        .from(DIGITAL_SPECIMEN)
        .where(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(specimenList))
        .fetch(this::mapToDigitalSpecimenRecord);
  }

  public void updateLastChecked(Set<String> currentDigitalSpecimen) {
    context.update(DIGITAL_SPECIMEN)
        .set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
        .where(DIGITAL_SPECIMEN.ID.in(currentDigitalSpecimen))
        .execute();
  }


  private DigitalSpecimenRecord mapToDigitalSpecimenRecord(Record dbRecord) {
    var digitalSpecimenWrapper = new DigitalSpecimenWrapper(
        dbRecord.get(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID),
        dbRecord.get(DIGITAL_SPECIMEN.TYPE),
        mapper.readValue(dbRecord.get(DIGITAL_SPECIMEN.DATA).data(), DigitalSpecimen.class),
        mapper.readTree(dbRecord.get(DIGITAL_SPECIMEN.ORIGINAL_DATA).data()));
    return new DigitalSpecimenRecord(dbRecord.get(DIGITAL_SPECIMEN.ID),
        digitalSpecimenWrapper, null);
  }

}


