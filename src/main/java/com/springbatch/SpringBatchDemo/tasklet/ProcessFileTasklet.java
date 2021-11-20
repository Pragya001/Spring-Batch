package com.springbatch.SpringBatchDemo.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class ProcessFileTasklet implements Tasklet {
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        System.out.println("Processing file step started .............");
        Thread.sleep(1000);
        System.out.println("Processing file step completed .............");
        return RepeatStatus.FINISHED;
    }
}
