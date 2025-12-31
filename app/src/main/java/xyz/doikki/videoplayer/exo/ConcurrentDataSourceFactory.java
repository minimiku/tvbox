package xyz.doikki.videoplayer.exo;

import androidx.media3.datasource.DataSource;

import com.github.tvbox.osc.util.HawkUtils;

/**
 * 多线程数据源工厂
 * 根据用户配置决定是否使用多线程缓冲
 */
public class ConcurrentDataSourceFactory implements DataSource.Factory {

    private final DataSource.Factory upstreamFactory;

    /**
     * 构造多线程数据源工厂
     *
     * @param upstreamFactory 上游数据源工厂（如 OkHttpDataSource.Factory）
     */
    public ConcurrentDataSourceFactory(DataSource.Factory upstreamFactory) {
        this.upstreamFactory = upstreamFactory;
    }

    @Override
    public DataSource createDataSource() {
        if (HawkUtils.getExoBufferMultithread()) {
            // 多线程缓冲已开启，使用 ConcurrentDataSource
            return new ConcurrentDataSource(upstreamFactory);
        } else {
            // 多线程缓冲关闭，使用原始数据源
            return upstreamFactory.createDataSource();
        }
    }
}
