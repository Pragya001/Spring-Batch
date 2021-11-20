package com.springbatch.SpringBatchDemo.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class InMemProcessor implements ItemProcessor<Integer, Integer> {

    @Override
    public Integer process(Integer item) throws Exception {
        return Integer.sum(10, item);
    }
}
