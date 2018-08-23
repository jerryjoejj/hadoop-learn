package com.lma.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private Long upFlow;
    private Long downFlow;
    private Long totalFlow;

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public void setTotalFlow(Long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public Long getTotalFlow() {
        return totalFlow;
    }

    public FlowBean(Long upFlow, Long downFlow, Long totalFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = totalFlow;
    }

    public FlowBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    public FlowBean() {
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", totalFlow=" + totalFlow +
                '}';
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.totalFlow);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        //反序列化顺序应与序列化顺序一致
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.totalFlow = dataInput.readLong();
    }
}
