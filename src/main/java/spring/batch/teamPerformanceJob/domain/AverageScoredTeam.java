package spring.batch.teamPerformanceJob.domain;

public record AverageScoredTeam(
        String name,
        double averageScore
) {
}
