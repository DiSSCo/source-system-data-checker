package eu.dissco.sourcesystemdatachecker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.sourcesystemdatachecker.domain.DigitalMediaEvent;
import eu.dissco.sourcesystemdatachecker.domain.DigitalSpecimenEvent;
import eu.dissco.sourcesystemdatachecker.properties.RabbitMqProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RabbitMqPublisherService {

  private final ObjectMapper mapper;
  private final RabbitTemplate rabbitTemplate;
  private final RabbitMqProperties rabbitMqProperties;

  public void deadLetterRaw(String event) {
    rabbitTemplate.convertAndSend(rabbitMqProperties.getDlq().getExchangeName(),
        rabbitMqProperties.getDlq().getRoutingKeyName(), event);
  }

  public void republishSpecimenEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getRepublish().getExchangeName(),
        rabbitMqProperties.getRepublish().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }

  public void publishSpecimenEvent(DigitalSpecimenEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getSpecimen().getExchangeName(),
        rabbitMqProperties.getSpecimen().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }

  public void publishMediaEvent(DigitalMediaEvent event) throws JsonProcessingException {
    rabbitTemplate.convertAndSend(
        rabbitMqProperties.getMedia().getExchangeName(),
        rabbitMqProperties.getMedia().getRoutingKeyName(), mapper.writeValueAsString(event)
    );
  }


}
