package eu.dissco.sourcesystemdatachecker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SourceSystemDataCheckerApplication {

  public static void main(String[] args) {
    SpringApplication.run(SourceSystemDataCheckerApplication.class, args);
  }

}
