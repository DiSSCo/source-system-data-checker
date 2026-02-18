package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;

public class ServiceUtils {
  private ServiceUtils(){
    // utility class
  }

  public static String getMediaUri(DigitalMediaEvent mediaEvent) {
    return mediaEvent.digitalMediaWrapper().attributes().getAcAccessURI();
  }

}
