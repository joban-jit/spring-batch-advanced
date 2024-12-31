package spring.batch.teamPerformanceJob;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
@EnableBatchProcessing

public class TestConfiguration {

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        // Set the JDBC URL for H2 (in-memory database)
        dataSource.setUrl("jdbc:h2:mem:testdb");

        // Set the H2 JDBC driver class name
        dataSource.setDriverClassName("org.h2.Driver");

        // Set the username and password for H2 (you can leave these as default for in-memory)
        dataSource.setUsername("sa");
        dataSource.setPassword("");

        return dataSource;
    }
}

