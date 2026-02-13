package eu.dissco.sourcesystemdatachecker.exception;

public class RabbitMqFailedPublishingRuntimeException extends RuntimeException {

  public RabbitMqFailedPublishingRuntimeException(String message) {
    super(message);
  }

}
