package cn.darkjrong.hdfs.exception;

import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.StrUtil;

/**
 * hdfs异常
 *
 * @author Rong.Jia
 * @date 2022/01/02
 */
public class HdfsException extends RuntimeException {

    public HdfsException(Throwable e) {
        super(ExceptionUtil.getMessage(e), e);
    }

    public HdfsException(String message) {
        super(message);
    }

    public HdfsException(String messageTemplate, Object... params) {
        super(StrUtil.format(messageTemplate, params));
    }

    public HdfsException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public HdfsException(Throwable throwable, String messageTemplate, Object... params) {
        super(StrUtil.format(messageTemplate, params), throwable);
    }

    public boolean causeInstanceOf(Class<? extends Throwable> clazz) {
        Throwable cause = this.getCause();
        return null != clazz && clazz.isInstance(cause);
    }

}
