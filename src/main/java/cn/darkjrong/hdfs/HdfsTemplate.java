package cn.darkjrong.hdfs;

import cn.darkjrong.hdfs.exception.HdfsException;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * hdfs 操作
 *
 * @author Rong.Jia
 * @date 2021/12/31
 */
public class HdfsTemplate {

    private static final Logger logger = LoggerFactory.getLogger(HdfsTemplate.class);
    private final FileSystem fileSystem;
    public HdfsTemplate(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * 检查文件、目录是否存在
     *
     * @param dirOrFile 文件、目录
     * @return {@link Boolean}
     */
    public Boolean exist(String dirOrFile) {

        Assert.notBlank(dirOrFile, "目录不能为空");
        Path path = new Path(dirOrFile);
        try {
            return fileSystem.exists(path);
        } catch (Exception e) {
            logger.error("exist {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 创建目录
     *
     * @param directory 目录
     * @return {@link Boolean}
     */
    public Boolean mkdirs(String directory) {
        return mkdirs(directory, FsPermission.getDirDefault());
    }

    /**
     * 创建目录
     *
     * @param directory  目录
     * @param permission 权限
     * @return {@link Boolean}
     */
    public Boolean mkdirs(String directory, FsPermission permission) {
        Assert.notBlank(directory, "目录不能为空");
        Path path = new Path(directory);
        try {
            return fileSystem.mkdirs(path, permission);
        } catch (Exception e) {
            logger.error("mkdirs {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 关闭
     */
    public void close() {
        try {
            fileSystem.close();
        } catch (Exception e) {
            logger.error("close {}", e.getMessage());
        }
    }

    /**
     * 返回当前用户在文件系统中的家目录
     * @return 目录
     */
    public Path getHomeDirectory() {
        return fileSystem.getHomeDirectory();
    }

    /**
     * 获取协议
     * @return 协议
     */
    public String getScheme() {
        return fileSystem.getScheme();
    }

    /**
     * 上传文件
     *
     * @param src       原文件
     * @param target    目标文件
     * @param delSrc    是否删除原文件
     * @param overwrite 是否覆盖目标文件
     * @return {@link Boolean}
     */
    private Boolean upload(File src, File target, boolean delSrc, boolean overwrite) {
        return upload(src.getPath(), target.getPath(), delSrc, overwrite);
    }

    /**
     * 上传文件
     *
     * @param src       原文件
     * @param target    目标文件
     * @param delSrc    是否删除原文件
     * @param overwrite 是否覆盖目标文件
     * @return {@link Boolean}
     */
    private Boolean upload(String src, String target, boolean delSrc, boolean overwrite) {
        Assert.isTrue(FileUtil.exist(src), String.format("待上传文件不存在, fileName : %s", src));
        try {
            fileSystem.copyFromLocalFile(delSrc, overwrite, new Path(src), new Path(target));
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("upload {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 上传文件
     *
     * @param src       原文件
     * @param target    目标文件
     * @param overwrite 是否覆盖目标文件
     * @return {@link Boolean}
     */
    public Boolean upload(File src, File target, boolean overwrite) {
        return upload(src.getPath(), target.getPath(), Boolean.FALSE, overwrite);
    }

    /**
     * 上传文件
     *
     * @param src       原文件
     * @param target    目标文件
     * @return {@link Boolean}
     */
    public Boolean upload(File src, File target) {
        return upload(src.getPath(), target.getPath(), Boolean.FALSE, Boolean.TRUE);
    }

    /**
     * 上传文件
     *
     * @param src       原文件
     * @param target    目标文件
     * @param overwrite 是否覆盖目标文件
     * @return {@link Boolean}
     */
    public Boolean upload(String src, String target, boolean overwrite) {
        return upload(src, target, Boolean.FALSE, overwrite);
    }

    /**
     * 上传文件
     *
     * @param src    原文件
     * @param target 目标文件
     * @return {@link Boolean}
     */
    public Boolean upload(String src, String target) {
        return upload(src, target, Boolean.TRUE);
    }

    /**
     * 合并文件
     *
     * @param psrcs  合并的文件
     * @param target 合并后目标位置
     * @return {@link Boolean}
     */
    public Boolean concat(List<String> psrcs, String target) {
        Assert.notEmpty(psrcs, "待合并文件不能为空");

        Path[] paths = new Path[psrcs.size()];
        for (int i = 0; i < psrcs.size(); i++) {
            paths[i] = new Path(psrcs.get(i));
        }

        try {
            fileSystem.concat(new Path(target), paths);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("concat {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 下载文件
     *
     * @param src                   hdfs文件
     * @param target                目标文件
     * @param delSrc                是否删除原文件
     * @param useRawLocalFileSystem 使用原始本地文件系统
     * @throws HdfsException hdfs异常
     */
    public void download(String src, String target, boolean delSrc,
                             boolean useRawLocalFileSystem) throws HdfsException {
        Assert.isTrue(exist(src), String.format("HDFS中不存在该文件, fileName: %s", src));
        try {
            fileSystem.copyToLocalFile(delSrc, new Path(src), new Path(target), useRawLocalFileSystem);
        } catch (Exception e) {
            logger.error("download {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 下载文件
     *
     * @param src                   hdfs文件
     * @param target                目标文件
     * @param delSrc                是否删除原文件
     * @throws HdfsException hdfs异常
     */
    public void download(String src, String target, boolean delSrc) throws HdfsException {
       download(src, target, delSrc, Boolean.FALSE);
    }

    /**
     * 下载文件
     *
     * @param src                   hdfs文件
     * @param target                目标文件
     * @throws HdfsException hdfs异常
     */
    public void download(String src, String target) throws HdfsException {
        download(src, target, Boolean.FALSE);
    }

    /**
     * 下载文件
     *
     * @param src                   hdfs文件
     * @param target                目标文件
     * @param delSrc                是否删除原文件
     * @throws HdfsException hdfs异常
     */
    public void download(File src, File target, boolean delSrc) throws HdfsException {
        download(src.getPath(), target.getPath(), delSrc);
    }

    /**
     * 下载文件
     *
     * @param src                   hdfs文件
     * @param target                目标文件
     * @throws HdfsException hdfs异常
     */
    public void download(File src, File target) throws HdfsException {
        download(src, target, Boolean.FALSE);
    }

    /**
     * 创建指定路径一个全新的0长度文件，如果创建失败或者文件已存在则返回false
     *
     * @param file 文件
     * @return {@link Boolean}
     */
    public Boolean createNewFile(String file) {
        try {
            return fileSystem.createNewFile(new Path(file));
        } catch (Exception e) {
            logger.error("createNewFile {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 创建快照
     *
     * @param file         文件
     * @param snapshotName 快照名称
     * @return {@link Path} 快照路径
     * @throws HdfsException hdfs异常
     */
    public Path createSnapshot(String file, String snapshotName) throws HdfsException {
        try {
            return fileSystem.createSnapshot(new Path(file), snapshotName);
        } catch (Exception e) {
            logger.error("createSnapshot {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 创建快照
     *
     * @param file         文件
     * @return {@link Path} 快照路径
     * @throws HdfsException hdfs异常
     */
    public Path createSnapshot(String file) throws HdfsException {
        return createSnapshot(file, null);
    }

    /**
     * 获取文件链接状态
     *
     * @param file 文件
     * @return {@link FileStatus} 文件链接状态
     * @throws HdfsException hdfs异常
     */
    public FileStatus getFileLinkStatus(String file) throws HdfsException{
        try {
            return fileSystem.getFileLinkStatus(new Path(file));
        } catch (Exception e) {
            logger.error("getFileStatus {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 删除目录, 文件
     *
     * @param dirOrFile 目录,文件
     * @param recursive 是否递归删除
     * @return {@link Boolean}
     */
    public Boolean delete(String dirOrFile, boolean recursive) {
        Assert.isTrue(exist(dirOrFile), String.format("待删除目录/文件不存在, directory : %s", dirOrFile));
        try {
            return fileSystem.delete(new Path(dirOrFile), recursive);
        } catch (Exception e) {
            logger.error("delete {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除目录,文件
     *
     * @param dirOrFile 目录,文件
     * @return {@link Boolean}
     */
    public Boolean delete(String dirOrFile) {
        return delete(dirOrFile, Boolean.FALSE);
    }

    /**
     * 文件系统关闭时删除
     *
     * @param dirOrFile dir或文件
     * @return {@link Boolean}
     */
    public Boolean deleteOnExit(String dirOrFile) {
        try {
            return fileSystem.deleteOnExit(new Path(dirOrFile));
        } catch (Exception e) {
            logger.error("deleteOnExit {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 删除快照
     *
     * @param file         文件
     * @param snapshotName 快照名称
     * @return {@link Boolean}
     */
    public Boolean deleteSnapshot(String file, String snapshotName) {
        try {
            fileSystem.deleteSnapshot(new Path(file), snapshotName);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("deleteSnapshot {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 删除快照
     *
     * @param file         文件
     * @return {@link Boolean}
     */
    public Boolean deleteSnapshot(String file) {
        return deleteSnapshot(file, null);
    }

    /**
     * 获得文件或者目录的acl状态
     *
     * @param path 路径
     * @return {@link AclStatus}
     */
    public AclStatus getAclStatus(String path) throws HdfsException {
        try {
            return fileSystem.getAclStatus(new Path(path));
        } catch (Exception e) {
            logger.error("getAclStatus {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 获取内容摘要
     *
     * @param path 路径
     * @return {@link ContentSummary}
     */
    public ContentSummary getContentSummary(String path) throws HdfsException {
        try {
            return fileSystem.getContentSummary(new Path(path));
        } catch (Exception e) {
            logger.error("getContentSummary {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 获取副本数目
     *
     * @param path 路径
     * @return int 数目
     */
    public int getDefaultReplication(String path) {
        return fileSystem.getDefaultReplication(new Path(path));
    }

    /**
     * 获取文件校验和
     *
     * @param path 路径
     * @return {@link FileChecksum}
     * @throws HdfsException hdfs异常
     */
    public FileChecksum getFileChecksum(String path) throws HdfsException {
        try {
            return fileSystem.getFileChecksum(new Path(path));
        } catch (Exception e) {
            logger.error("getFileChecksum {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 获取文件系统的使用和容量的状态对象
     *
     * @param path 路径
     * @return {@link FsStatus}
     * @throws HdfsException hdfs异常
     */
    public FsStatus getStatus(String path) throws HdfsException {
        try {
            return fileSystem.getStatus(new Path(path));
        } catch (Exception e) {
            logger.error("getStatus {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 获取文件系统的使用和容量的状态对象
     *
     * @return {@link FsStatus}
     * @throws HdfsException hdfs异常
     */
    public FsStatus getStatus() throws HdfsException {
       return getStatus(null);
    }

    /**
     * 获取文件系统中所有文件的总大小
     *
     * @return {@link Long} 总大小
     */
    public Long getUsed() {
        try {
            return fileSystem.getUsed();
        } catch (Exception e) {
            logger.error("getUsed {}", e.getMessage());
        }

        return Long.MAX_VALUE;
    }

    /**
     * 获取工作目录
     *
     * @return {@link String}
     */
    public String getWorkingDirectory() {
        return fileSystem.getWorkingDirectory().toString();
    }

    /**
     * 获取子目录的所有文件或目录
     *
     * @param directory 目录
     * @return {@link List}<{@link FileStatus}>
     * @throws HdfsException hdfs异常
     */
    public List<FileStatus> listStatus(String directory) throws HdfsException {
        Assert.isTrue(exist(directory), String.format("目录不存在, directory:  %s", directory));
        try {
            return CollectionUtil.newArrayList(fileSystem.listStatus(new Path(directory)));
        } catch (Exception e) {
            logger.error("listStatus {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 路径列表中过滤文件/目录
     *
     * @param fileList 文件列表
     * @return {@link List}<{@link FileStatus}>
     * @throws HdfsException hdfs异常
     */
    public List<FileStatus> listStatus(List<String> fileList) throws HdfsException {
        return listStatus(fileList, path -> true);
    }

    /**
     * 路径列表中过滤文件/目录
     *
     * @param fileList 文件列表
     * @param filter   过滤器
     * @return {@link List}<{@link FileStatus}>
     */
    public List<FileStatus> listStatus(List<String> fileList, PathFilter filter) {
        Assert.notEmpty(fileList, "文件列表不能为空");
        Path[] files = new Path[fileList.size()];
        for (int i = 0; i < fileList.size(); i++) {
            files[i] = new Path(fileList.get(i));
        }
        try {
            return CollectionUtil.newArrayList(fileSystem.listStatus(files, filter));
        } catch (IOException e) {
            logger.error("listStatus {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 路径列表中过滤文件/目录
     *
     * @param path   路径
     * @param filter 过滤器
     * @return {@link List}<{@link FileStatus}>
     */
    public List<FileStatus> listStatus(String path, PathFilter filter){
        return listStatus(CollectionUtil.newArrayList(path), filter);
    }

    /**
     * 获取一个文件或者目录的扩展属性名
     *
     * @param path 路径
     * @return {@link List}<{@link String}>
     */
    public List<String> listXAttrs(String path) throws HdfsException {

        Assert.isTrue(exist(path), String.format("文件不存在, fileName : %s", path));
        try {
            return fileSystem.listXAttrs(new Path(path));
        } catch (Exception e) {
            logger.error("listXAttrs {}", e.getMessage());
            throw new HdfsException(e);
        }
    }

    /**
     * 修改acl条目
     *
     * @param path    路径
     * @param aclSpec acl规范
     * @return {@link Boolean}
     */
    public Boolean modifyAclEntries(String path, List<AclEntry> aclSpec) {
        Assert.isTrue(exist(path), String.format("文件不存在, fileName : %s", path));
        try {
            fileSystem.modifyAclEntries(new Path(path), aclSpec);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("modifyAclEntries {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 删除acl
     *
     * @param path 路径
     * @return {@link Boolean}
     */
    public Boolean removeAcl(String path) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.removeAcl(new Path(path));
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("removeAcl {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 从文件或者目录中删除所有的ACL条目
     *
     * @param path    路径
     * @param aclSpec acl规范
     * @return {@link Boolean}
     */
    public Boolean removeAclEntries(String path, List<AclEntry> aclSpec) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.removeAclEntries(new Path(path), aclSpec);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("removeAclEntries {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 从文件或者目录中删除所有的ACL条目
     *
     * @param path 路径
     * @return {@link Boolean}
     */
    public Boolean removeDefaultAcl(String path) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.removeDefaultAcl(new Path(path));
            return Boolean.TRUE;
        } catch (IOException e) {
            logger.error("removeDefaultAcl {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 重命名
     *
     * @param src src
     * @param dst dst
     * @return {@link Boolean}
     */
    public Boolean rename(String src, String dst) {
        Assert.isTrue(exist(src), String.format("文件/目录不存在, src : %s", src));
        try {
            return fileSystem.rename(new Path(src), new Path(dst));
        } catch (Exception e) {
            logger.error("rename {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 重命名快照
     *
     * @param path            路径
     * @param snapshotOldName 快照旧名称
     * @param snapshotNewName 快照新名字
     * @return {@link Boolean}
     */
    public Boolean renameSnapshot(String path, String snapshotOldName, String snapshotNewName) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.renameSnapshot(new Path(path), snapshotOldName, snapshotNewName);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("renameSnapshot {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 丢弃所有现在的条目，完全替代文件或者目录的ACL条目
     *
     * @param path    路径
     * @param aclSpec acl规范
     * @return {@link Boolean}
     */
    public Boolean setAcl(String path, List<AclEntry> aclSpec) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.setAcl(new Path(path), aclSpec);
            return Boolean.TRUE;
        } catch (IOException e) {
            logger.error("setAcl {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 设置path的拥有者
     *
     * @param path      路径
     * @param username  用户名
     * @param groupName 组名称
     * @return {@link Boolean}
     */
    public Boolean setOwner(String path, String username, String groupName) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));

        try {
            fileSystem.setOwner(new Path(path), username, groupName);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setOwner {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 设置权限
     *
     * @param path       路径
     * @param permission 许可
     * @return {@link Boolean}
     */
    public Boolean setPermission(String path, FsPermission permission) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            fileSystem.setPermission(new Path(path), permission);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setPermission {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 为现存的文件设置副本数
     *
     * @param src       路径
     * @param replication 副本数
     * @return {@link Boolean}
     */
    public Boolean setReplication(String src, short replication) {
        Assert.isTrue(exist(src), String.format("文件不存在, path : %s", src));
        try {
            fileSystem.setReplication(new Path(src), replication);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setReplication {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 设置文件的访问时间
     *
     * @param path  路径
     * @param mtime 文件的修改时间，值为-1表示该调用不应该设置修改时间, 单位：毫秒
     * @param atime 文件的访问时间, -1表示该调用不应该设置访问时间, 单位：毫秒
     * @return {@link Boolean}
     */
    public Boolean setTimes(String path, long mtime, long atime) {
        Assert.isTrue(exist(path), String.format("文件不存在, path : %s", path));
        try {
            fileSystem.setTimes(new Path(path), mtime, atime);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setTimes {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 设置验证校验和标志
     *
     * @param verifyChecksum 验证校验和标志
     * @return {@link Boolean}
     */
    public Boolean setVerifyChecksum(boolean verifyChecksum) {
        try {
            fileSystem.setVerifyChecksum(verifyChecksum);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("setVerifyChecksum {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 设置写校验标志
     *
     * @param writeChecksum 写校验标志
     * @return {@link Boolean}
     */
    public Boolean setWriteChecksum(boolean writeChecksum) {
        try {
            fileSystem.setWriteChecksum(writeChecksum);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("setWriteChecksum {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 对文件/目录取消退出时删除
     *
     * @param path 路径
     * @return {@link Boolean}
     */
    public Boolean cancelDeleteOnExit(String path) {
        Assert.isTrue(exist(path), String.format("文件/目录不存在, path : %s", path));
        try {
            return fileSystem.cancelDeleteOnExit(new Path(path));
        }catch (Exception e) {
            logger.error("cancelDeleteOnExit {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


}
