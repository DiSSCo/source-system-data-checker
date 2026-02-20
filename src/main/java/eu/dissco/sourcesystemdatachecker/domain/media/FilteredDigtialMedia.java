package eu.dissco.sourcesystemdatachecker.domain.media;

import java.util.Set;

public record FilteredDigtialMedia(
    Set<DigitalMediaEvent> newOrChangedMedia,
    Set<DigitalMediaRecord> unchangedMedia
) {

}
