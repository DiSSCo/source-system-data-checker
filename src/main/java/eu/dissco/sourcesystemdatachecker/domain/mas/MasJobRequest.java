package eu.dissco.sourcesystemdatachecker.domain.mas;

public record MasJobRequest(
    String masId,
    String targetId,
    boolean batching,
    String agentId,
    MjrTargetType targetType
) {

}
