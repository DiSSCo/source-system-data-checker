package eu.dissco.sourcesystemdatachecker.repository;

import static eu.dissco.sourcesystemdatachecker.TestUtils.CREATED;
import static eu.dissco.sourcesystemdatachecker.TestUtils.MAPPER;
import static eu.dissco.sourcesystemdatachecker.TestUtils.PHYSICAL_ID_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.SPECIMEN_DOI_1;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.sourcesystemdatachecker.database.jooq.Tables.DIGITAL_SPECIMEN;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenRecord;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpecimenRepositoryIT extends BaseRepositoryIT {

  private SpecimenRepository repository;

  @BeforeEach
  void setup() {
    repository = new SpecimenRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(DIGITAL_SPECIMEN).execute();
  }

  @Test
  void testGetSpecimen(){
    // Given
    var expected = new DigitalSpecimenRecord(
        SPECIMEN_DOI_1,
        givenDigitalSpecimenWrapper(PHYSICAL_ID_1, false, Set.of()),
        null
    );
    insertSpecimen(expected);

    // When
    var result = repository.getDigitalSpecimens(Set.of(PHYSICAL_ID_1));

    // Then
    assertThat(result).isEqualTo(List.of(expected));
  }

  private void insertSpecimen(DigitalSpecimenRecord specimenRecord){
    context.insertInto(DIGITAL_SPECIMEN)
        .set(DIGITAL_SPECIMEN.ID, specimenRecord.id())
        .set(DIGITAL_SPECIMEN.TYPE, "ods:DigitalSpecimen")
        .set(DIGITAL_SPECIMEN.VERSION, 1)
        .set(DIGITAL_SPECIMEN.MIDSLEVEL, (short) 1)
        .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID, specimenRecord.digitalSpecimenWrapper().physicalSpecimenId())
        .set(DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE, "PreservedSpecimen")
        .set(DIGITAL_SPECIMEN.SPECIMEN_NAME, "name")
        .set(DIGITAL_SPECIMEN.ORGANIZATION_ID, "https://ror.org/aaa")
        .set(DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID, "https://hdl.handle.net/20.1025.5000/AAA")
        .set(DIGITAL_SPECIMEN.CREATED, CREATED)
        .set(DIGITAL_SPECIMEN.LAST_CHECKED, Instant.now())
        .set(DIGITAL_SPECIMEN.MODIFIED, Instant.now())
        .set(DIGITAL_SPECIMEN.DATA, JSONB.valueOf(
            MAPPER.valueToTree(specimenRecord.digitalSpecimenWrapper().attributes())
                    .toString().replace("\\u0000", "")))
        .set(DIGITAL_SPECIMEN.ORIGINAL_DATA,
            JSONB.valueOf(
                specimenRecord.digitalSpecimenWrapper().originalAttributes().toString()))
        .execute();
  }

}
