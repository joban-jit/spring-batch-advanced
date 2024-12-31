package spring.batch.teamPerformanceJob.processor;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import spring.batch.teamPerformanceJob.domain.AverageScoredTeam;
import spring.batch.teamPerformanceJob.domain.Team;

import static spring.batch.teamPerformanceJob.CommonConstants.*;
/**
 * Processor for calculating average score for a team by the specified score rank.
 * As a side effect, puts best and worst player's score and name (in the specified score rank)
 * in a step-specific execution context
 */
@RequiredArgsConstructor
public class TeamAverageProcessor implements ItemProcessor<Team, AverageScoredTeam> {

    private final int scoreRank;

    @Setter
    private StepExecution stepExecution;

    @Override
    public AverageScoredTeam process(Team team) throws Exception {
        if (stepExecution == null) {
            throw new RuntimeException("Team average processor can not execute without step execution set");
        }
        ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
        Double maxScore =  stepExecutionContext.containsKey(MAX_SCORE)?stepExecutionContext.getDouble(MAX_SCORE):null;
        Double minScore = stepExecutionContext.containsKey(MIN_SCORE) ? stepExecutionContext.getDouble(MIN_SCORE) : null;

        double sum = 0;
        double count = 0;

        for(Team.ScoredPlayer scoredPlayer : team.getScoredPlayers())
        {
            Double score = scoredPlayer.getScores().get(scoreRank);
            if(maxScore==null || score> maxScore){
                stepExecutionContext.putDouble(MAX_SCORE, score);
                stepExecutionContext.putString(MAX_PLAYER, scoredPlayer.getName());
                maxScore = score;
            }
            if (minScore == null || score < minScore) {
                stepExecutionContext.putDouble(MIN_SCORE, score);
                stepExecutionContext.putString(MIN_PLAYER, scoredPlayer.getName());
                minScore = score;
            }

            sum += score;
            count++;
        }
        return new AverageScoredTeam(team.getName(), sum/count);

    }
}
