package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.TestUtils.MAPPER;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEvent;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.sourcesystemdatachecker.properties.RabbitMqProperties;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;


@Testcontainers
@ExtendWith(MockitoExtension.class)
class RabbitMqPublisherServiceTest {

  private static RabbitMQContainer container;
  private static RabbitTemplate rabbitTemplate;
  private static final RabbitMqProperties rabbitMqProperties = new RabbitMqProperties();
  private RabbitMqPublisherService rabbitMqPublisherService;

  @BeforeAll
  static void setupContainer() throws IOException, InterruptedException {
    container = new RabbitMQContainer("rabbitmq:4.0.8-management-alpine");
    container.start();
    declareRabbitResources(rabbitMqProperties.getRepublish().getRoutingKeyName());
    declareRabbitResources(rabbitMqProperties.getNameUsage().getRoutingKeyName());
    declareRabbitResources(rabbitMqProperties.getMedia().getRoutingKeyName());
    CachingConnectionFactory factory = new CachingConnectionFactory(container.getHost());
    factory.setPort(container.getAmqpPort());
    factory.setUsername(container.getAdminUsername());
    factory.setPassword(container.getAdminPassword());
    rabbitTemplate = new RabbitTemplate(factory);
    rabbitTemplate.setReceiveTimeout(100L);
  }

  private static void declareRabbitResources(String routingKey)
      throws IOException, InterruptedException {
    var exchangeName = routingKey + "-exchange";
    var queueName = routingKey + "-queue";
    container.execInContainer("rabbitmqadmin", "declare", "exchange", "name=" + exchangeName,
        "type=direct", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "queue", "name=" + queueName,
        "queue_type=quorum", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "binding", "source=" + exchangeName,
        "destination_type=queue", "destination=" + queueName, "routing_key=" + routingKey);
  }

  @BeforeEach
  void setup() {
    rabbitMqPublisherService = new RabbitMqPublisherService(MAPPER, rabbitTemplate,
        rabbitMqProperties);
  }

  @Test
  void testRepublishEvent()  {
    // Given

    // When
    rabbitMqPublisherService.republishEvent(givenDigitalSpecimenEvent());

    // Then
    var result =
        rabbitTemplate.receive(rabbitMqProperties.getRepublish().getRoutingKeyName() + "-queue");
    assertThat(result.getBody()).isNotNull();
  }

  @Test
  void testPublishNameUsageEvent()  {
    // Given

    // When
    rabbitMqPublisherService.publishNameUsageEvent(givenDigitalSpecimenEvent());

    // Then
    var result =
        rabbitTemplate.receive(rabbitMqProperties.getNameUsage().getRoutingKeyName() + "-queue");
    assertThat(result.getBody()).isNotNull();
  }

  @Test
  void testPublishMediaEvent()  {
    // Given

    // When
    rabbitMqPublisherService.publishMediaEvent(givenDigitalMediaEvent());

    // Then
    var result =
        rabbitTemplate.receive(rabbitMqProperties.getMedia().getRoutingKeyName() + "-queue");
    assertThat(result.getBody()).isNotNull();
  }

}
