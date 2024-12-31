package spring.batch.teamPerformanceJob.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CommandRunner;
import org.springframework.batch.core.step.tasklet.JvmCommandRunner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.teamPerformanceJob.CommonConstants;
import spring.batch.teamPerformanceJob.domain.AverageScoredTeam;
import spring.batch.teamPerformanceJob.domain.Team;
import spring.batch.teamPerformanceJob.domain.TeamPerformance;
import spring.batch.teamPerformanceJob.processor.TeamAverageProcessor;
import spring.batch.teamPerformanceJob.reader.DivisionFileReader;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Configuration
public class TeamPerformanceJobConfiguration {


    @Value("classpath:input/*.txt")
    private Resource[] inDivisionResources;

    @Value("file:calculated/avg.txt")
    private WritableResource outAvgResource;

    @Value("file:calculated/max.txt")
    private WritableResource maxPerformanceRatioOutResource;

    @Value("file:calculated/min.txt")
    private WritableResource minPerformanceRatioOutResource;

    @Value("file:calculated/")
    private WritableResource calculatedDirectoryResource;

    @Bean
    @Qualifier("teamPerformanceJob")
    public Job teamPerformanceJob(
            JobRepository jobRepository,
            @Qualifier("threadPoolTaskExecutor") TaskExecutor threadPoolTaskExecutor,
            @Qualifier("averageTeamScoreStep") Step averageTeamScoreStep,
            @Qualifier("teamMaxRatioPerformanceStep") Step teamMaxRatioPerformanceStep,
            @Qualifier("teamMinRatioPerformanceStep") Step teamMinRatioPerformanceStep,
            @Qualifier("shellScriptStep") Step shellScriptStep,
            @Qualifier("successLoggerStep") Step successLoggerStep


    ) {
        // Wrap both performance steps into corresponding flows
        Flow maxRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("maxRatioPerformanceFlow")
                .start(teamMaxRatioPerformanceStep)
                .build();
        Flow minRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("minRatioPerformanceFlow")
                .start(teamMinRatioPerformanceStep)
                .build();
        // Split flow to execute min and max ratio performance flows in parallel
        Flow performanceSplitFlow = new FlowBuilder<SimpleFlow>("performanceSplitFlow")
                .split(threadPoolTaskExecutor)
                .add(maxRatioPerformanceFlow, minRatioPerformanceFlow)
                .build();
        return new JobBuilder("teamPerformanceJob", jobRepository)
                // 1. (Start) Flow with single step -> average team score
                // (flow is needed since the next is split flow, not a step)
                .start(new FlowBuilder<SimpleFlow>("averageTeamScoreFlow")
                        .start(averageTeamScoreStep)
                        .build())
                // 2. Next is parallel flow with 2 performance steps running in parallel
                .next(performanceSplitFlow)
                // 3. Execute shell script after done with parallel performance steps
                .next(shellScriptStep)
                // 4. Last step is to execute logging the success step
                .next(successLoggerStep)
                .build()
                .build();

    }

    @Bean
    @Qualifier("averageTeamScoreStep")
    public Step averageTeamScoreStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("divisionTeamReader") ItemReader<Team> divisionTeamReader,
            @Qualifier("teamAverageProcessor") TeamAverageProcessor teamAverageProcessor,
            @Qualifier("jobStartLoggerListener") StepExecutionListener jobStartLoggerListener,
            @Qualifier("teamAverageContextPromotionListener") ExecutionContextPromotionListener teamAverageContextPromotionListener
    ) {
        return new StepBuilder("averageTeamScoreStep", jobRepository)
                .<Team, AverageScoredTeam>chunk(1, transactionManager)
                .reader(divisionTeamReader)
                .processor(teamAverageProcessor)
                .writer(
                        new FlatFileItemWriterBuilder<AverageScoredTeam>()
                                .name("averageTeamScoreWriter")
                                .resource(outAvgResource)
                                .delimited()
                                .delimiter(",")
                                .fieldExtractor(avgScoredTeam -> new Object[]{avgScoredTeam.name(), avgScoredTeam.averageScore()})
                                .build()
                )
                // This step should log the informational message
                .listener(jobStartLoggerListener)
                // Listener to promote step execution context to job execution context
                .listener(teamAverageContextPromotionListener)
                // / Listener to inject step execution object before step started and to flush it after it finished
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(null);
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .faultTolerant()
                .skip(IndexOutOfBoundsException.class)
                .noSkip(NullPointerException.class)
                .skipLimit(40)
                .build();
    }

    @Bean
    @Qualifier("teamMaxRatioPerformanceStep")
    public Step teamMaxRatioPerformanceStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("maxRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor,
            @Qualifier("maxHeaderWriter") FlatFileHeaderCallback maxHeaderWriter
    ) {
        return new StepBuilder("teamMaxRatioPerformanceStep", jobRepository)
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                .reader(averageScoredTeamReader())
                .processor(maxRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMinRatioPerformanceWriter")
                        .resource(maxPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(teamPerformance -> new Object[]{
                                teamPerformance.name(), teamPerformance.performance()
                        })
                        .headerCallback(maxHeaderWriter)
                        .build()
                )
                .build();

    }

    @Bean
    @Qualifier("teamMinRatioPerformanceStep")
    public Step teamMinRatioPerformanceStep(JobRepository jobRepository,
                                            PlatformTransactionManager transactionManager,
                                            @Qualifier("minRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor,
                                            @Qualifier("minHeaderWriter") FlatFileHeaderCallback minHeaderWriter) {
        return new StepBuilder("teamMinRatioPerformanceStep", jobRepository)
                // Read-and-write one-by-one
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                // Reading from average scored team file
                .reader(averageScoredTeamReader())
                .processor(minRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMinRatioPerformanceWriter")
                        .resource(minPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[]{team.name(), team.performance()})
                        .headerCallback(minHeaderWriter)
                        .build())
                .build();
    }

    @Bean("shellScriptStep")
    public Step shellScriptStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("shellScriptTasklet") Tasklet shellScriptTasklet
    ){
        return new StepBuilder("shellScriptStep", jobRepository)
                .tasklet(shellScriptTasklet, transactionManager)
                .build();
    }


    @Bean("successLoggerStep")
    public Step successLoggerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("successLoggerTasklet") Tasklet successLoggerTasklet
    ){
        return new StepBuilder("successLoggerStep", jobRepository)
                .tasklet(successLoggerTasklet, transactionManager)
                .build();
    }



    @Bean
    @Qualifier("divisionTeamReader")
    public ItemReader<Team> divisionTeamReader() {
        FlatFileItemReader<String> lineReader = new FlatFileItemReaderBuilder<String>()
                .name("divisionLineReader")
                .lineMapper((line, lineNumber) -> line)
                .build();
        DivisionFileReader singleFileMultiLineReader = new DivisionFileReader(lineReader);

        return new MultiResourceItemReaderBuilder<Team>()
                .name("divisionTeamReader")
                .delegate(singleFileMultiLineReader)
                .resources(inDivisionResources)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("teamAverageProcessor")
    public TeamAverageProcessor teamAverageProcessor(@Value("#{jobParameters['scoreRank']}") int scoreRank) {
        return new TeamAverageProcessor(scoreRank);
    }

    // job launcher
    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher taskExecutorJobLauncher = new TaskExecutorJobLauncher();
        taskExecutorJobLauncher.setJobRepository(jobRepository);
        taskExecutorJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return taskExecutorJobLauncher;
    }


    @Bean
    @StepScope
    @Qualifier("jobStartLoggerListener")
    public StepExecutionListener jobStartLoggerListener(
            @Value("#{jobParameters['uuid']}") String uuid
    ) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                log.info("Job with uuid = {} is started", uuid);
            }
        };
    }

    @Bean
    @StepScope
    @Qualifier("teamAverageContextPromotionListener")
    public ExecutionContextPromotionListener teamAverageContextPromotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{
                CommonConstants.MAX_SCORE,
                CommonConstants.MAX_PLAYER,
                CommonConstants.MIN_SCORE,
                CommonConstants.MIN_PLAYER
        });
        return listener;
    }

    // Create new instance of file item reader: defined as a separate method to be reused,
    // however not defined as a bean to have a separate instance per each step (executed in parallel)
    public ItemReader<AverageScoredTeam> averageScoredTeamReader() {
        return new FlatFileItemReaderBuilder<AverageScoredTeam>()
                .name("averageScoredTeamReader")
                .resource(outAvgResource)
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .fieldSetMapper(fieldSet -> new AverageScoredTeam(fieldSet.readString(0), fieldSet.readDouble(1)))
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("maxRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor(

            @Value("#{jobExecutionContext['max.score']}") double maxScore
    ) {
        return item -> evaluatePerformance(item, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor(@Value("#{jobExecutionContext['min.score']}") double minScore) {
        return item -> evaluatePerformance(item, minScore);
    }

    private static TeamPerformance evaluatePerformance(AverageScoredTeam team, double baselineScore) {
        BigDecimal performance = BigDecimal.valueOf(team.averageScore())
                .multiply(new BigDecimal(100))
                .divide(BigDecimal.valueOf(baselineScore), 2, RoundingMode.HALF_UP);
        return new TeamPerformance(team.name(), performance.toString() + "%");
    }

    @Bean
    @StepScope
    @Qualifier("maxHeaderWriter")
    public FlatFileHeaderCallback maxHeaderWriter(
            @Value("#{jobExecutionContext['max.score']}") double maxScore,
            @Value("#{jobExecutionContext['max.player']}") String maxPlayerName
    ) {
        return writer -> writerHeader(writer, maxPlayerName, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minHeaderWriter")
    public FlatFileHeaderCallback minHeaderWriter(@Value("#{jobExecutionContext['min.score']}") double minScore,
                                                  @Value("#{jobExecutionContext['min.player']}") String minPlayerName) {
        return writer -> writerHeader(writer, minPlayerName, minScore);
    }


    private static void writerHeader(Writer writer, String name, double score) {
        try {
            writer.write("******************************************************\n");
            writer.write("Team performance below are calculated against " + score + " which was scored by " + name + "\n");
            writer.write("******************************************************\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    @Qualifier("threadPoolTaskExecutor")
    public TaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolExecutor = new ThreadPoolTaskExecutor();
        // 2-thread size pool to execute split flow with 2 parallel steps
        threadPoolExecutor.setCorePoolSize(2);
        return threadPoolExecutor;
    }


    @Bean("shellScriptTasklet")
    @StepScope
    public Tasklet shellScriptTasklet(
            @Value("#{jobParameters['uuid']}") String uuid
    ){
        return ( contribution, chunkContext)->{
            CommandRunner commandRunner = new JvmCommandRunner();
            commandRunner.exec(
                    new String[]{"bash", "-l", "-c", "touch " + uuid + ".resulted"},
                    new String[]{},
                    calculatedDirectoryResource.getFile()
                    );
            return RepeatStatus.FINISHED;
        };
    }

    @Bean("successLoggerTasklet")
    @StepScope
    public Tasklet successLoggerTasklet(
            @Value("#{jobParameters['uuid']}") String uuid
    ){
        return (contribution, chuckContext)->{
            log.info("Job with uuid = {} is finished", uuid);
            return RepeatStatus.FINISHED;
        };
    }



//    @Bean
//    public DataSource dataSource(
//            @Value("${postgres.db.username}") String username,
//            @Value("${postgres.db.password}") String password,
//            @Value("${postgres.db.url}") String url,
//            @Value("${postgres.db.driverClassName}") String driverClassName
//    ){
//        DriverManagerDataSource dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName(driverClassName);
//        dataSource.setUsername(username);
//        dataSource.setPassword(password);
//        dataSource.setUrl(url);
//        return dataSource;
//    }


//    @Bean
//    public BatchProperties batchProperties(
//            @Value("${batch.db.initialize-schema}}")DatabaseInitializationMode initializationMode
//    ){
//        BatchProperties properties = new BatchProperties();
//        properties.getJdbc().setInitializeSchema(initializationMode);
//        return properties;
//    }
//
//    @Bean
//    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(
//            DataSource dataSource,
//            BatchProperties batchProperties
//    ){
//        return new BatchDataSourceScriptDatabaseInitializer(dataSource, batchProperties.getJdbc());
//    }

}
