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
public class VideoBean implements WritableComparable<VideoBean> {
    private String videoId;
    private String uploader;
    private Integer age;
    private Integer category;
    private Integer length;
    private Integer views;
    private Double rate;
    private Integer ratings;
    private Integer conments;
    private String relatedIds;

    @Override
    public int compareTo(VideoBean o) {
        return this.views > o.getViews() ? -1 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(videoId);
        dataOutput.writeUTF(uploader);
        dataOutput.writeInt(age);
        dataOutput.writeInt(category);
        dataOutput.writeInt(length);
        dataOutput.writeInt(views);
        dataOutput.writeDouble(rate);
        dataOutput.writeInt(ratings);
        dataOutput.writeInt(conments);
        dataOutput.writeUTF(relatedIds);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        videoId = dataInput.readUTF();
        uploader = dataInput.readUTF();
        age= dataInput.readInt();
        category = dataInput.readInt();
        length = dataInput.readInt();
        views =dataInput.readInt();
        rate = dataInput.readDouble();
        conments = dataInput.readInt();
        conments = dataInput.readInt();
        relatedIds = dataInput.readUTF();
    }
}
