package org.example.batch.graphical.source;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

public class QeInputFormat extends RichInputFormat<String, GenericInputSplit> {


    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int i) throws IOException {
        return new GenericInputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] genericInputSplits) {
        return null;
    }

    @Override
    public void open(GenericInputSplit genericInputSplit) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public String nextRecord(String s) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
