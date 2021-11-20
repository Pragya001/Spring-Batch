package com.springbatch.SpringBatchDemo.reader;

import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class InMemReader extends AbstractItemStreamItemReader {
    Integer[] myInts = {1, 2, 3, 4, 5, 6, 7};
    List<Integer> myList = Arrays.asList(myInts);

    int index = 0;

    @Override
    public Object read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Integer nextItem = null;
        if( index < myList.size()) {
            nextItem = myList.get(index++);
        } else {
            index = 0;
        }
        return nextItem;
    }
}
