package com.techprimers.springbatchexample1.config;

import com.techprimers.springbatchexample1.batch.partition.ColumnRangePartitioner;
import com.techprimers.springbatchexample1.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class PartitionBatchConfig {
    public final JobBuilderFactory jobBuilderFactory;
    public final StepBuilderFactory stepBuilderFactory;
    public final ItemReader<User> itemReader;
    public final ItemProcessor<User, User> itemProcessor;
    public final ItemWriter<User> itemWriter;

    private int chunkSize;

    @Value("${chunkSize:100}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    private int poolSize;

    @Value("${poolSize:10}")
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    private int minValue;

    @Value("${minValue:1}")
    public void setMinValue(int minValue) {
        this.minValue = minValue;
    }

    private int maxValue;

    @Value("${maxValue:100}")
    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }

    @Bean(name = "partitionJob")
    public Job partitionJob(){
        return jobBuilderFactory.get("partitionJob")
                .incrementer(new RunIdIncrementer())
                .start(partitionStep())
                .build();
    }

    @Bean(name = "partitionStep")
    public Step partitionStep() {
        return stepBuilderFactory.get("partitionStep")
                .partitioner("partitionStep", partitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Partitioner partitioner() {

        return new ColumnRangePartitioner(minValue, maxValue);
    }

    @Bean(name = "workerStep")
    public Step workerStep() {
        return stepBuilderFactory.get("workerStep")
                .<User,User> chunk(5)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();

    }

    @Bean(name="partitionHandler")
    public TaskExecutorPartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setGridSize(poolSize);
        handler.setTaskExecutor(taskExecutor());
        handler.setStep(workerStep());

        return handler;
    }

    @Bean(name = "taskExecutor")
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setThreadNamePrefix("partition-thread");
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }
}
