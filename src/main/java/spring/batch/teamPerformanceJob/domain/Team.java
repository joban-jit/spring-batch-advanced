package spring.batch.teamPerformanceJob.domain;

import java.util.LinkedList;
import java.util.List;

// Entity representing single team for team performance job; has name and list of players, each has list of scores
public class Team {

    private final String name;
    private final List<ScoredPlayer> scoredPlayers;

    public Team(String name) {
        this.name = name;
        this.scoredPlayers = new LinkedList<>();
    }

    public String getName() {
        return name;
    }

    public List<ScoredPlayer> getScoredPlayers() {
        return scoredPlayers;
    }

    // Auxiliary entity representing player with the list of scores
    public static class ScoredPlayer {

        private final String name;
        // Technically scores could be in any order, but according to file format, will be in descending order
        private final List<Double> scores;

        public ScoredPlayer(String name) {
            this.name = name;
            this.scores = new LinkedList<>();
        }

        public String getName() {
            return name;
        }

        public List<Double> getScores() {
            return scores;
        }
    }
}
