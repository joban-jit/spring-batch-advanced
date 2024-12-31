package spring.batch.teamPerformanceJob.reader;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import spring.batch.teamPerformanceJob.domain.Team;

import java.util.Optional;

@RequiredArgsConstructor
public class DivisionFileReader implements ResourceAwareItemReaderItemStream<Team> {

    private final FlatFileItemReader<String> delegateReader;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // Make sure that resource specific calls are propagated to the delegate
        delegateReader.open(executionContext);
    }


    @Override
    public void close() throws ItemStreamException {
        // Make sure that resource specific calls are propagated to the delegate
        delegateReader.close();
    }

    @Override
    public void setResource(Resource resource) {
        // Make sure that resource specific calls are propagated to the delegate
        delegateReader.setResource(resource);
    }


    @Override
    public Team read() throws Exception {
        // Create reference to the team (as optional), such that it's shared across line reads
        Optional<Team> maybeTeam = Optional.empty();
        String line;

        // Iterate over lines until team record is completed or EOF reached
        while ((line = delegateReader.read()) != null) {
            line = line.trim(); // Removing spaces left and right
            if (line.isEmpty()) { // Empty line designate end-of-record
                return maybeTeam.orElse(null);
            } else if (!line.contains(":")) { // No colon means that team name is listed
                maybeTeam = Optional.of(new Team(line));
            } else { // Otherwise, we have a colon, and it's a sign that it's player's description
                final String[] nameAndScores = line.split(":");
                maybeTeam.ifPresent(team -> team.getScoredPlayers().add(parseScoredPlayer(nameAndScores)));
            }
        }

        // It's possible that we've already accumulated an item, so EOF should not lose the progress
        // and team entity should be returned. This will cause DivisionFileReader.read() to be called again
        // and return null (in this case, team reference will be null), which is perfectly valid
        return maybeTeam.orElse(null);
    }

    private Team.ScoredPlayer parseScoredPlayer(String[] nameAndScores){
        String name = nameAndScores[0];
        String[] scores = nameAndScores[1].split(",");
        Team.ScoredPlayer scoredPlayer = new Team.ScoredPlayer(name);
        for (String score: scores){
            scoredPlayer.getScores().add(Double.parseDouble(score));
        }
        return scoredPlayer;
    }
}
