package eu.dissco.sourcesystemdatachecker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMqConsumerService {

  private final ObjectMapper mapper;
  private final SourceSystemDataCheckerService sourceSystemDataCheckerService;
  private final RabbitMqPublisherService publisherService;

  @RabbitListener(queues = {
      "${rabbitmq.queue-name:source-system-data-checker-queue}"}, containerFactory = "consumerBatchContainerFactory")
  public void getMessages(@Payload List<String> messages)
      throws JsonProcessingException {

    var events = messages.stream().map(message -> {
      try {
        return mapper.readValue(message, DigitalSpecimenEvent.class);
      } catch (JsonProcessingException e) {
        log.error("Moving message to DLQ, failed to parse event message", e);
        publisherService.deadLetterRaw(message);
        return null;
      }
    }).filter(Objects::nonNull).toList();
    sourceSystemDataCheckerService.handleMessages(events);

  }

}
