package org.project.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintFunction extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(PrintFunction.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // Printing the tuple to the console
        log.info(tuple.toString());
    }
}
