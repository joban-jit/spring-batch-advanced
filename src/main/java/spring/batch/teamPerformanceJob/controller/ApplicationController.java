package spring.batch.teamPerformanceJob.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import spring.batch.teamPerformanceJob.CommonConstants;

import java.util.UUID;

@RestController
public class ApplicationController {

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher jobLauncher;

    @Autowired
    private Job teamPerformanceJob;

    @PostMapping("/start")
    public String start(@RequestParam("scoreRank") int scoreRank) throws Exception{
        String uuid = UUID.randomUUID().toString();
        launchJobAsynchronously(scoreRank, uuid);
        return "Job with id "+uuid+" was submitted";
    }

    private void launchJobAsynchronously(int scoreRank, String uuid) throws Exception{
       jobLauncher.run(teamPerformanceJob, new JobParametersBuilder()
               .addLong(CommonConstants.SCORE_RANK_PARAM, (long) scoreRank)
               .addString(CommonConstants.UUID_PARAM, uuid)
               .toJobParameters());
    }

}
