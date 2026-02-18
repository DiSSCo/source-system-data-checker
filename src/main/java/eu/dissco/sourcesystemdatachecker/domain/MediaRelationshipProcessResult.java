package eu.dissco.sourcesystemdatachecker.domain;

import eu.dissco.sourcesystemdatachecker.schema.EntityRelationship;
import java.util.Collections;
import java.util.List;

public record MediaRelationshipProcessResult(
    List<EntityRelationship> tombstonedRelationships,
    List<DigitalMediaEvent> newLinkedObjects,
    List<EntityRelationship> unchangedRelationships
) {
  public MediaRelationshipProcessResult() {
    this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }
}
