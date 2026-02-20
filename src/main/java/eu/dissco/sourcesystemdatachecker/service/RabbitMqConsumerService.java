package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.specimen.DigitalSpecimenEvent;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMqConsumerService {

  private final JsonMapper mapper;
  private final SourceSystemDataCheckerService sourceSystemDataCheckerService;

  @RabbitListener(queues = {
      "${rabbitmq.queue-name:source-system-data-checker-queue}"}, containerFactory = "consumerBatchContainerFactory")
  public void getMessages(@Payload List<String> messages) {
    var events = messages.stream()
        .map(message -> mapper.readValue(message, DigitalSpecimenEvent.class))
        .filter(Objects::nonNull).collect(Collectors.toSet());
    sourceSystemDataCheckerService.handleMessages(events);
  }

}
