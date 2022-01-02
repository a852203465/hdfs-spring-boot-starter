package cn.darkjrong.spring.boot.autoconfigure;

import cn.darkjrong.hdfs.HdfsTemplate;
import cn.hutool.core.util.ObjectUtil;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * hdfs工厂bean
 *
 * @author Rong.Jia
 * @date 2021/12/31
 */
public class HdfsFactoryBean implements FactoryBean<HdfsTemplate>, InitializingBean, DisposableBean {

    private HdfsTemplate hdfsTemplate;
    private FileSystem fileSystem;

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public HdfsTemplate getObject() {
        return this.hdfsTemplate;
    }

    @Override
    public Class<?> getObjectType() {
        return HdfsTemplate.class;
    }

    @Override
    public boolean isSingleton() {
        return Boolean.TRUE;
    }

    @Override
    public void destroy() {
        if (ObjectUtil.isNotNull(hdfsTemplate)) {
            this.hdfsTemplate.close();
        }
    }

    @Override
    public void afterPropertiesSet() {
        hdfsTemplate = new HdfsTemplate(fileSystem);
    }
}
