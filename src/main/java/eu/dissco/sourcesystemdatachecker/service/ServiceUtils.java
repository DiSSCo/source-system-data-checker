package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.media.DigitalMediaEvent;

public class ServiceUtils {

  private ServiceUtils() {
    // Utility class
  }

  protected static final String DOI_PROXY = "https://doi.org/";

  protected static String getAccessUri(DigitalMediaEvent mediaEvent) {
    return mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI();
  }

}
