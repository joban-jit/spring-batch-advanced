package spring.batch.teamPerformanceJob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import spring.batch.teamPerformanceJob.config.TeamPerformanceJobConfiguration;
import spring.batch.teamPerformanceJob.domain.AverageScoredTeam;
import spring.batch.teamPerformanceJob.domain.TeamPerformance;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBatchTest
@SpringJUnitConfig({TestConfiguration.class, TeamPerformanceJobConfiguration.class})
public class RatioPerformanceProcessorTest {
    private static final String TEAM_NAME = UUID.randomUUID().toString();
    private static final double TEAM_SCORE = 4d;
    private static final String EXPECTED_MIN_PERFORMANCE = "200.00%";
    private static final double MIN_SCORE = 2d;
    private static final String EXPECTED_MAX_PERFORMANCE = "80.00%";
    private static final double MAX_SCORE = 5d;

    @Autowired
    @Qualifier("maxRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor;

    @Autowired
    @Qualifier("minRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor;

    @Test
    public void testProcessorsUsingSharedStepExecution() throws Exception {
        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, TEAM_SCORE);
        TeamPerformance minPerformance = minRatioPerformanceProcessor.process(team);
        Assertions.assertNotNull(minPerformance);
        Assertions.assertEquals(TEAM_NAME, minPerformance.name());
        Assertions.assertEquals(EXPECTED_MIN_PERFORMANCE, minPerformance.performance());

        TeamPerformance maxPerformance = maxRatioPerformanceProcessor.process(team);
        Assertions.assertNotNull(maxPerformance);
        Assertions.assertEquals(TEAM_NAME, maxPerformance.name());
        Assertions.assertEquals(EXPECTED_MAX_PERFORMANCE, maxPerformance.performance());
    }


//     Custom step execution is provided for common use across all tests
    public StepExecution getStepExecution() {
        // Mocking job execution context parameters
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(CommonConstants.MIN_SCORE, MIN_SCORE);
        context.putDouble(CommonConstants.MAX_SCORE, MAX_SCORE);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        return stepExecution;
    }

    @Test
    public void testProcessorsUsingCustomStepExecution() throws Exception {
        // Creating custom step execution mock to use it
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(CommonConstants.MIN_SCORE, 1d);
        context.putDouble(CommonConstants.MAX_SCORE, 2d);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, 4d);

        // We make sure to bring-our-own step execution
        TeamPerformance minPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> minRatioPerformanceProcessor.process(team));
        Assertions.assertNotNull(minPerformance);
        Assertions.assertEquals(TEAM_NAME, minPerformance.name());
        Assertions.assertEquals("400.00%", minPerformance.performance());

        // We make sure to bring-our-own step execution
        TeamPerformance maxPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> maxRatioPerformanceProcessor.process(team));
        Assertions.assertNotNull(maxPerformance);
        Assertions.assertEquals(TEAM_NAME, maxPerformance.name());
        Assertions.assertEquals("200.00%", maxPerformance.performance());
    }

}
