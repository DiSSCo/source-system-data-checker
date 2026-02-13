package eu.dissco.sourcesystemdatachecker.service;

import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.properties.RabbitMqProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMqPublisherService {

  private final JsonMapper mapper;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMqProperties rabbitMqProperties;

  public void republishEvent(DigitalSpecimenEvent event) {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getRepublish().getExchangeName(),
        rabbitMqProperties.getRepublish().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }

  public void publishNameUsageEvent(DigitalSpecimenEvent event) {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getNameUsage().getExchangeName(),
        rabbitMqProperties.getNameUsage().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }

  public void publishMediaEvent(DigitalMediaEvent event) {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getMedia().getExchangeName(),
        rabbitMqProperties.getMedia().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }

}
