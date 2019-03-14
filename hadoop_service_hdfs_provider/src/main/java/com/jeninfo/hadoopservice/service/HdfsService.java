package com.jeninfo.hadoopservice.service;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author chenzhou
 * @Date 2019/3/10 17:30
 * @Description
 */
@Service
public class HdfsService {
    @Autowired
    private FileSystem fileSystem;

    /**
     * 递归查看文件详情
     *
     * @param path
     * @throws Exception
     */
    public List<LocatedFileStatus> readFile(String path) throws Exception {
        List<LocatedFileStatus> files = new ArrayList<>();
        // 2 执行查看文件详情操作
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(path), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            files.add(status);
        }
        // 3 关闭资源
        fileSystem.close();
        return files;
    }

    /**
     * 查看目录
     *
     * @param path
     * @throws IOException
     */
    public void getFileSystem(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        Arrays.stream(fileStatuses).forEach((file) -> {
            if (file.isFile()) {
                System.out.println("文件---" + file.getPath().getName());
            } else {
                System.out.println("文件夹--" + file.getPath().getName());
            }
        });
    }

    /**
     * 文件上传
     *
     * @param localPath
     * @param hdfsPath
     * @throws IOException
     */
    public void putFileToHdfs(String localPath, String hdfsPath) throws IOException {
        fileSystem.copyFromLocalFile(true, new Path(localPath), new Path(hdfsPath));
        fileSystem.close();
    }

    /**
     * 下载文件
     *
     * @param localPath
     * @param hdfsPath
     * @throws Exception
     */
    public void getFileFromHdfs(String localPath, String hdfsPath) throws Exception {
        fileSystem.copyToLocalFile(false, new Path(hdfsPath), new Path(localPath), true);
        fileSystem.close();
    }

    /**
     * 读取文件内容
     *
     * @param path
     * @throws Exception
     */
    public void readContext(String path) throws Exception {
        // 打开文件输入流
        FSDataInputStream in = fileSystem.open(new Path(path));
        IOUtils.copyBytes(in, System.out, 4096, false);
        // 关闭流
        IOUtils.closeStream(in);
    }

    /**
     * 创建文件夹
     *
     * @param hdfsPath
     * @param permission
     * @throws Exception
     */
    public void mkdirAtHdfs(String hdfsPath, FsPermission permission) throws Exception {
        if (permission != null) {
            fileSystem.mkdirs(new Path(hdfsPath), permission);

        } else {
            fileSystem.mkdirs(new Path(hdfsPath));
        }
        fileSystem.close();
    }

    /**
     * 文件重命名
     *
     * @param oldPath
     * @param newPath
     * @return
     * @throws Exception
     */
    public boolean fileReName(String oldPath, String newPath) throws Exception {
        return fileSystem.rename(new Path(oldPath), new Path(newPath));
    }

    /**
     * 文件删除
     *
     * @param filePath
     * @return
     * @throws Exception
     */
    public boolean deleteFile(String filePath) throws Exception {
        return fileSystem.deleteOnExit(new Path(filePath));
    }

    /**
     * 目录删除
     *
     * @return
     * @throws Exception
     */
    public boolean deleteDirectory(String directPath) throws Exception {
        Path path = new Path(directPath);
        return fileSystem.delete(path, true);
    }
}
