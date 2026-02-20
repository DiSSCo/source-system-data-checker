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

  private NameUsage nameUsage = new NameUsage();
  private Media media = new Media();
  private MasScheduler masScheduler = new MasScheduler();

  @Data
  @Validated
  public static class NameUsage {

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
  public static class MasScheduler {

    @NotBlank
    private String exchangeName = "mas-scheduler-exchange";

    @NotNull
    private String routingKeyName = "mas-scheduler";
  }

}
