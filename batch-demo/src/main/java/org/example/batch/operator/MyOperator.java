package org.example.batch.operator;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;

/**
 * 自定义操作算子：将字符串转为数字并乘以2返回
 */
public class MyOperator<T> extends SingleInputUdfOperator<T, T, MyOperator<T>> {

    private Function function;

    protected MyOperator(DataSet<T> input, Function function) {
        super(input, input.getType());
        this.function = function;
    }

    protected Function getFunction() {
        return this.function;
    }

    protected Operator<T> translateToDataFlow(Operator<T> input) {
        
        return null;
    }
}
