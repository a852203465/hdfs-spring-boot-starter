package cn.darkjrong.hdfs.config;

import cn.darkjrong.spring.boot.autoconfigure.HdfsProperties;
import cn.hutool.core.convert.Convert;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * hdfs配置
 *
 * @author Rong.Jia
 * @date 2022/01/02
 */
@Configuration
public class HdfsConfig {

    private final HdfsProperties hdfsProperties;

    public HdfsConfig(HdfsProperties hdfsProperties) {
        this.hdfsProperties = hdfsProperties;
    }

    /**
     * 获取HDFS配置信息
     * @return HDFS配置信息
     */
    @Bean
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", hdfsProperties.getServerAddress());
        configuration.set("dfs.replication", Convert.toStr(hdfsProperties.getReplication()));
        return configuration;
    }

    /**
     * 创建HDFS文件系统对象
     * @return HDFS文件系统对象
     * @throws Exception 创建异常
     */
    @Bean
    public FileSystem createFileSystem(org.apache.hadoop.conf.Configuration configuration) throws Exception {

        /*
         客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份
         DHADOOP_USER_NAME=hadoop
         也可以在构造客户端fs对象时，通过参数传递进去
         */

        return FileSystem.get(new URI(hdfsProperties.getServerAddress()), configuration, hdfsProperties.getUsername());
    }


}
