package com.jeninfo.hadoopservice.mr.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chenzhou
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
public class OrderBean implements WritableComparable<OrderBean> {

    private int ordeId;
    private double price;

    @Override
    public int compareTo(OrderBean o) {
        int result = ordeId > o.getOrdeId() ? 1 : -1;

        if (ordeId > o.getOrdeId()) {
            result = 1;
        } else if (ordeId < o.getOrdeId()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(ordeId);
        dataOutput.writeDouble(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ordeId = dataInput.readInt();
        price = dataInput.readDouble();
    }
}
