package eu.dissco.sourcesystemdatachecker.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    rabbitTemplate.convertAndSend(rabbitMqProperties.getDlqExchangeName(),
        rabbitMqProperties.getDlqRoutingKeyName(), event);
  }

}
