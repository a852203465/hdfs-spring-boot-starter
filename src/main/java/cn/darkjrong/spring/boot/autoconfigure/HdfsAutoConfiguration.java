package cn.darkjrong.spring.boot.autoconfigure;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * hdfs 配置
 *
 * @author Rong.Jia
 * @date 2021/12/31
 */
@Configuration
@ComponentScan("cn.darkjrong.hdfs")
@ConditionalOnProperty(prefix = "hdfs", name = "enabled", havingValue = "true")
@EnableConfigurationProperties({HdfsProperties.class})
public class HdfsAutoConfiguration {

    @Bean
    public HdfsFactoryBean hdfsFactoryBean(FileSystem fileSystem) {
        HdfsFactoryBean hdfsFactoryBean = new HdfsFactoryBean();
        hdfsFactoryBean.setFileSystem(fileSystem);
        return hdfsFactoryBean;
    }














}
