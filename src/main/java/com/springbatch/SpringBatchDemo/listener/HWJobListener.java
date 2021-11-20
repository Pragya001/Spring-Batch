package com.springbatch.SpringBatchDemo.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HWJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Before Job listener " + jobExecution.getJobId() + " " + jobExecution.getJobParameters() +" " + jobExecution.getJobInstance().getJobName());
        System.out.println("Execution Context " + jobExecution.getExecutionContext());
        jobExecution.getExecutionContext().put("pragya", "Smart");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("After Job listener " + jobExecution.getJobId() +" " +  jobExecution.getJobParameters() +" " + jobExecution.getJobInstance().getJobName());
        System.out.println("Execution Context " + jobExecution.getExecutionContext());
    }
}
