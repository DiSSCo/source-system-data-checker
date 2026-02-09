package eu.dissco.sourcesystemdatachecker.service;

import static eu.dissco.sourcesystemdatachecker.TestUtils.MAPPER;
import static eu.dissco.sourcesystemdatachecker.TestUtils.givenDigitalSpecimenEvent;
import static org.mockito.BDDMockito.then;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RabbitMqConsumerServiceTest {

  private RabbitMqConsumerService consumerService;
  @Mock
  private SourceSystemDataCheckerService service;

  @BeforeEach
  void setup(){
    consumerService = new RabbitMqConsumerService(MAPPER, service);
  }

  @Test
  void testHandleMessages() throws Exception {
    // Given
    var message = MAPPER.writeValueAsString(givenDigitalSpecimenEvent());

    // When
    consumerService.getMessages(List.of(message));

    // Then
    then(service).should().handleMessages(List.of(givenDigitalSpecimenEvent()));
  }

}
