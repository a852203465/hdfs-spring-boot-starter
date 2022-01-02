package cn.darkjrong.spring.boot.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * HDFS 属性
 *
 * @author Rong.Jia
 * @date 2019/12/28 21:09
 */
@Data
@ConfigurationProperties(prefix = "hdfs")
public class HdfsProperties {

    /**
     *  是否开启，默认false
     */
    private boolean enabled = Boolean.FALSE;

    /**
     * HDFS服务器地址
     */
    private String serverAddress;

    /**
     * HDFS 目录
     */
    private String namespace;

    /**
     *  复制块数
     */
    private Integer replication;

    /**
     * 用户名
     */
    private String username;




}
