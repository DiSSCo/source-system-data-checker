package eu.dissco.sourcesystemdatachecker.repository;

import static eu.dissco.sourcesystemdatachecker.database.jooq.Tables.DIGITAL_SPECIMEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenWrapper;
import eu.dissco.sourcesystemdatachecker.exception.DisscoJsonBMappingException;
import eu.dissco.sourcesystemdatachecker.exception.DisscoRepositoryException;
import eu.dissco.sourcesystemdatachecker.schema.DigitalSpecimen;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class SpecimenRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public List<DigitalSpecimenRecord> getDigitalSpecimens(Set<String> specimenList) {
    return context.select(DIGITAL_SPECIMEN.asterisk())
        .from(DIGITAL_SPECIMEN)
        .where(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID.in(specimenList))
        .fetch(this::mapToDigitalSpecimenRecord);
  }

  private DigitalSpecimenRecord mapToDigitalSpecimenRecord(Record dbRecord) {
    var digitalSpecimenWrapper = new DigitalSpecimenWrapper(
        dbRecord.get(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID),
        dbRecord.get(DIGITAL_SPECIMEN.TYPE),
        mapToDigitalSpecimen(dbRecord.get(DIGITAL_SPECIMEN.DATA)),
        mapToJson(dbRecord.get(DIGITAL_SPECIMEN.ORIGINAL_DATA)));
    return new DigitalSpecimenRecord(dbRecord.get(DIGITAL_SPECIMEN.ID),
        dbRecord.get(DIGITAL_SPECIMEN.MIDSLEVEL), dbRecord.get(DIGITAL_SPECIMEN.VERSION),
        dbRecord.get(DIGITAL_SPECIMEN.CREATED), digitalSpecimenWrapper, null, null, null, null);
  }

  private DigitalSpecimen mapToDigitalSpecimen(JSONB jsonb) {
    try {
      return mapper.readValue(jsonb.data(), DigitalSpecimen.class);
    } catch (JsonProcessingException e) {
      log.warn("Unable to map jsonb to digital specimen: {}", jsonb.data(), e);
      return new DigitalSpecimen();
    }
  }

  private JsonNode mapToJson(JSONB jsonb) {
    try {
      return mapper.readValue(jsonb.data(), JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new DisscoJsonBMappingException("Failed to parse jsonb field to json: " + jsonb.data(),
          e);
    }
  }


}


