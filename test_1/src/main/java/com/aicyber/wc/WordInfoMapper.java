package com.aicyber.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author: xiong
 * @create at 2020/12/14
 */
public class WordInfoMapper implements FlatMapFunction<String, WordInfo> {

    @Override
    public void flatMap(String value, Collector<WordInfo> out) throws Exception {
        String[] words = value.split(" ");
        for (String word: words) {
            out.collect(new WordInfo(word,1));
        }
    }
}
