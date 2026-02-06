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

  private Specimen specimen = new Specimen();
  private Media media = new Media();
  private Dlq dlq = new Dlq();
  private Republish republish = new Republish();

  @Data
  @Validated
  public static class Republish {

    @NotBlank
    private String exchangeName = "source-system-data-checker-exchange";

    @NotBlank
    private String routingKeyName = "source-system-data-checker";
  }


  @Data
  @Validated
  public static class Specimen {

    @NotBlank
    private String exchangeName = "nu-search-exchange";

    @NotNull
    private String routingKeyName = "nu-search";
  }


  @Data
  @Validated
  public static class Media {

    @NotBlank
    private String exchangeName = "digital-media-exchange";

    @NotNull
    private String routingKeyName = "digital-media";
  }

  @Data
  @Validated
  public static class Dlq {

    @NotNull
    private String exchangeName = "source-system-data-checker-exchange-dlq";

    @NotNull
    private String routingKeyName = "source-system-data-checker-queue-dlq";

  }


}
