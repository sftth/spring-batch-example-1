package com.techprimers.springbatchexample1.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
public class PartitionController {

    @Autowired
    JobLauncher jobLauncher;

    @Resource(name = "partitionJob")
    Job job;

    @GetMapping("partition/load")
    public BatchStatus load() throws JobParametersInvalidException, JobExecutionException {
        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));

        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(job, parameters);

        log.info("JobExecution: " + jobExecution.getStatus());

        log.info("Batch is Running...");

        while (jobExecution.isRunning()) {
            log.info("...");
        }

        return jobExecution.getStatus();
    }

}
