package eu.dissco.sourcesystemdatachecker.domain;

import java.util.Set;

public record FilteredDigtialMedia(
    Set<DigitalMediaEvent> newOrChangedMedia,
    Set<DigitalMediaRecord> unchangedMedia
) {

}
