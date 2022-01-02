package cn.darkjrong.hdfs;

import cn.darkjrong.hdfs.config.HdfsConfig;
import cn.darkjrong.spring.boot.autoconfigure.HdfsFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.HdfsProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.AclStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * hdfs操作测试
 *
 * @author Rong.Jia
 * @date 2021/12/31
 */
public class HdfsTemplateTest {

    private HdfsTemplate hdfsTemplate;

    @BeforeEach
    public void setUp() throws Exception {

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setNamespace("/data");
        hdfsProperties.setEnabled(Boolean.TRUE);
        hdfsProperties.setReplication(1);
        hdfsProperties.setUsername("Mr.J");
        hdfsProperties.setServerAddress("hdfs://localhost:9000");

        HdfsConfig hdfsConfig = new HdfsConfig(hdfsProperties);
        FileSystem fileSystem = hdfsConfig.createFileSystem(hdfsConfig.getConfiguration());
        HdfsFactoryBean hdfsFactoryBean = new HdfsFactoryBean();
        hdfsFactoryBean.setFileSystem(fileSystem);
        hdfsFactoryBean.afterPropertiesSet();

        hdfsTemplate = hdfsFactoryBean.getObject();

    }

    @Test
    public void init() {
        System.out.println("init");
    }

    @Test
    public void exist() {
        System.out.println(hdfsTemplate.exist("/data/3.jpg"));
    }

    @Test
    public void mkdirDirectory() {
        System.out.println(hdfsTemplate.mkdirs("/data2/demo"));
    }

    @Test
    public void getHomeDirectory() {
        System.out.println(hdfsTemplate.getHomeDirectory());
    }

    @Test
    public void getScheme() {
        System.out.println(hdfsTemplate.getScheme());
    }

    @Test
    public void uploadFile() {
        System.out.println(hdfsTemplate.upload("F:/我的图片/美女/1.jpg", "/data/1.jpg"));
    }

    @Test
    public void uploadFile2() {

        File src = new File("F:/我的图片/美女/1.jpg");
        File target = new File("/data/2.jpg");

        System.out.println(hdfsTemplate.upload(src, target));
    }

    @Test
    public void downloadFile() {

        File target = new File("F:/1.jpg");
        File src = new File("/data/2.jpg");

        hdfsTemplate.download(src, target);

    }

    @Test
    public void delete() {

        System.out.println(hdfsTemplate.delete("/data2/1.jpg"));


    }

    @Test
    public void getAclStatus() {
        AclStatus aclStatus = hdfsTemplate.getAclStatus("/data/2.jpg");

        System.out.println(aclStatus.toString());
    }

    @Test
    public void getDefaultReplication() {
        System.out.println(hdfsTemplate.getDefaultReplication("/data/2.jpg"));
    }

    @Test
    public void getFileChecksum() {
        System.out.println(hdfsTemplate.getFileChecksum("/data/2.jpg"));

    }

    @Test
    public void getStatus() {
        System.out.println(hdfsTemplate.getStatus("/data/2.jpg"));

    }

    @Test
    public void getUsed() {
        System.out.println(hdfsTemplate.getUsed());
    }

    @Test
    public void getWorkingDirectory() {

        System.out.println(hdfsTemplate.getWorkingDirectory());
    }

    @Test
    public void listXAttrs() {
        System.out.println(hdfsTemplate.listXAttrs("/data/2.jpg"));
    }









}
