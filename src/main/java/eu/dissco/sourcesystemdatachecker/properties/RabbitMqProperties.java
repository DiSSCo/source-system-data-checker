package eu.dissco.sourcesystemdatachecker.properties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "rabbitmq")
public class RabbitMqProperties {

  @Positive
  private int batchSize = 500;

  @NotBlank
  private String exchangeName = "nu-search-exchange";

  @NotNull
  private String routingKeyName = "nu-search";

  @NotNull
  private String dlqExchangeName = "source-system-data-checker-exchange-dlq";

  @NotNull
  private String dlqRoutingKeyName = "source-system-data-checker-queue-dlq";

}
