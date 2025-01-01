package spring.batch.teamPerformanceJob;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
@EnableBatchProcessing
@PropertySource("classpath:application.properties")
public class TestConfiguration {

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    @Bean
    public DataSource dataSource(
            @Value("${spring.datasource.driverClassName}") String driverClassName,
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password
    ){
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource
                .setDriverClassName(driverClassName);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        return dataSource;
    }

}

