package com.jeninfo.hadoopservice.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/14 10:22
 * @Description
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class OrderBean implements WritableComparable<OrderBean> {


    private int ordeId;
    private double price;

    @Override
    public int compareTo(OrderBean o) {
        int result;
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

    @Override
    public String toString() {
        return ordeId + "\t" + price;
    }
}
