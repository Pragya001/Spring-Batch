package com.springbatch.SpringBatchDemo.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class HWStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Before step listener " + stepExecution.getStepName() + " " + stepExecution.getJobParameters() +" " + stepExecution.getJobExecution().getJobInstance().getJobName());
        System.out.println("Execution Context " + stepExecution.getExecutionContext());
//        stepExecution.getExecutionContext().put("pragya", "Smart");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("After step listener " + stepExecution.getStepName() + " " + stepExecution.getJobParameters() +" " + stepExecution.getJobExecution().getJobInstance().getJobName());
        System.out.println("Execution Context " + stepExecution.getExecutionContext());
        return null;
    }
}
