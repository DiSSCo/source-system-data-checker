package eu.dissco.sourcesystemdatachecker.configuration;

import eu.dissco.sourcesystemdatachecker.component.MessageCompressionComponent;
import eu.dissco.sourcesystemdatachecker.properties.RabbitMqProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class RabbitMqConfiguration {

  private final MessageCompressionComponent compressedMessageConverter;
  private final RabbitMqProperties rabbitMQProperties;

  @Bean
  public SimpleRabbitListenerContainerFactory consumerBatchContainerFactory(
      ConnectionFactory connectionFactory) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setBatchListener(true);
    factory.setBatchSize(rabbitMQProperties.getBatchSize());
    factory.setConsumerBatchEnabled(true);
    factory.setMessageConverter(compressedMessageConverter);
    return factory;
  }

  @Bean
  public RabbitTemplate compressedTemplate(ConnectionFactory connectionFactory,
      MessageCompressionComponent compressedMessageConverter) {
    var rabbitTemplate = new RabbitTemplate(connectionFactory);
    rabbitTemplate.setMessageConverter(compressedMessageConverter);
    return rabbitTemplate;
  }

}
