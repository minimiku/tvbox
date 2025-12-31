package xyz.doikki.videoplayer.exo;

import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.media3.datasource.DataSource;
import androidx.media3.datasource.DataSpec;
import androidx.media3.datasource.TransferListener;

import com.github.tvbox.osc.util.HawkUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多线程并发数据源
 * 用于 HLS 分片预加载，通过多线程加速缓冲
 *
 * 工作原理：
 * 1. 使用 HlsSegmentManager 获取已解析的分片列表
 * 2. 当加载某个分片时，同时预加载后续分片
 * 3. 预加载的数据缓存在内存中，播放时直接读取
 */
public class ConcurrentDataSource implements DataSource {

    private static final String TAG = "ConcurrentDataSource";
    private static final int DEFAULT_PRELOAD_COUNT = 3; // 默认预加载后续 3 个分片
    
    // 全局静态线程池
    private static volatile ExecutorService sSharedExecutor;
    private static final Object sExecutorLock = new Object();

    private final DataSource.Factory upstreamFactory;
    private final int threadCount;
    private final int preloadCount;
    private final ConcurrentHashMap<String, PreloadedSegment> preloadedSegments;
    private final AtomicBoolean isReleased;

    private DataSource currentDataSource;
    private android.net.Uri currentUri;
    private PreloadedSegment currentPreloaded; // 当前正在使用的预加载数据
    private int currentReadPosition; // 当前读取位置

    public ConcurrentDataSource(DataSource.Factory upstreamFactory) {
        this.upstreamFactory = upstreamFactory;
        this.threadCount = Math.min(HawkUtils.getExoBufferThreadCount(), 8);
        this.preloadCount = Math.min(threadCount, DEFAULT_PRELOAD_COUNT);
        this.preloadedSegments = new ConcurrentHashMap<>();
        this.isReleased = new AtomicBoolean(false);

        synchronized (sExecutorLock) {
            if (sSharedExecutor == null || sSharedExecutor.isShutdown()) {
                sSharedExecutor = Executors.newFixedThreadPool(8);
            }
        }

        BufferStatusManager.getInstance().setMultithreadEnabled(true, this.threadCount);
        Log.d(TAG, "ConcurrentDataSource 已创建, 线程数: " + this.threadCount + ", 预加载数: " + this.preloadCount);
    }

    @Override
    public void addTransferListener(@NonNull TransferListener transferListener) {
        // 可选：转发给底层数据源
    }

    @Override
    public long open(@NonNull DataSpec dataSpec) throws IOException {
        if (isReleased.get()) {
            throw new IOException("DataSource is released");
        }

        this.currentUri = dataSpec.uri;
        String uriString = currentUri.toString();
        this.currentReadPosition = 0;
        this.currentPreloaded = null;

        // 1. 检查是否有预加载的数据
        PreloadedSegment preloaded = preloadedSegments.remove(uriString);
        if (preloaded != null && preloaded.isReady()) {
            Log.d(TAG, "命中预加载缓存: " + getShortUrl(uriString) + ", 大小: " + preloaded.data.length);
            BufferStatusManager.getInstance().onPreloadHit(uriString);
            this.currentPreloaded = preloaded;
            
            // 调度下一批预加载
            scheduleNextPreloads(uriString);
            
            return preloaded.data.length;
        }
        
        BufferStatusManager.getInstance().onPreloadMiss(uriString);

        // 2. 清理过期的预加载任务
        cleanupOldPreloads(uriString);

        // 3. 正常打开数据源
        currentDataSource = upstreamFactory.createDataSource();
        long length = currentDataSource.open(dataSpec);

        // 4. 调度预加载
        scheduleNextPreloads(uriString);

        return length;
    }

    @Override
    public int read(@NonNull byte[] buffer, int offset, int length) throws IOException {
        // 如果使用预加载数据
        if (currentPreloaded != null) {
            byte[] data = currentPreloaded.data;
            int remaining = data.length - currentReadPosition;
            if (remaining <= 0) {
                return -1; // EOF
            }
            int toRead = Math.min(length, remaining);
            System.arraycopy(data, currentReadPosition, buffer, offset, toRead);
            currentReadPosition += toRead;
            return toRead;
        }
        
        // 否则从网络读取
        return currentDataSource.read(buffer, offset, length);
    }

    @Nullable
    @Override
    public android.net.Uri getUri() {
        return currentUri;
    }

    @NonNull
    @Override
    public Map<String, java.util.List<String>> getResponseHeaders() {
        if (currentPreloaded != null) {
            return new java.util.HashMap<>();
        }
        return currentDataSource != null ? currentDataSource.getResponseHeaders() : new java.util.HashMap<>();
    }

    @Override
    public void close() throws IOException {
        currentPreloaded = null;
        currentReadPosition = 0;
        if (currentDataSource != null) {
            currentDataSource.close();
            currentDataSource = null;
        }
    }

    public void release() {
        if (isReleased.compareAndSet(false, true)) {
            Log.d(TAG, "释放 ConcurrentDataSource");
            // 取消所有预加载任务
            for (PreloadedSegment segment : preloadedSegments.values()) {
                segment.cancel();
            }
            preloadedSegments.clear();
            HlsSegmentManager.getInstance().clearCache();
        }
    }

    private void scheduleNextPreloads(String currentUrl) {
        if (isReleased.get() || !HawkUtils.getExoBufferMultithread()) return;

        // 从分片管理器获取后续分片
        List<String> nextSegments = HlsSegmentManager.getInstance().getNextSegments(currentUrl, preloadCount);
        
        if (nextSegments.isEmpty()) {
            // 如果没有找到，尝试用正则推断（作为后备方案）
            for (int i = 1; i <= preloadCount; i++) {
                String nextUrl = predictNextUrl(currentUrl, i);
                if (nextUrl != null && !preloadedSegments.containsKey(nextUrl)) {
                    startPreload(nextUrl);
                }
            }
        } else {
            for (String nextUrl : nextSegments) {
                if (!preloadedSegments.containsKey(nextUrl)) {
                    startPreload(nextUrl);
                }
            }
        }
    }

    private void startPreload(String url) {
        if (preloadedSegments.size() >= threadCount * 2) {
            return; // 限制缓存数量
        }

        PreloadedSegment segment = new PreloadedSegment(url, upstreamFactory);
        preloadedSegments.put(url, segment);
        segment.future = sSharedExecutor.submit(segment);
        
        BufferStatusManager.getInstance().onPreloadScheduled(url);
        Log.d(TAG, "调度预加载: " + getShortUrl(url));
    }

    private void cleanupOldPreloads(String currentUrl) {
        // 清理不再需要的预加载数据（保留当前和后续3个）
        List<String> nextSegments = HlsSegmentManager.getInstance().getNextSegments(currentUrl, preloadCount + 1);
        
        java.util.Iterator<Map.Entry<String, PreloadedSegment>> it = preloadedSegments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PreloadedSegment> entry = it.next();
            String url = entry.getKey();
            if (!url.equals(currentUrl) && !nextSegments.contains(url)) {
                entry.getValue().cancel();
                it.remove();
            }
        }
    }

    /**
     * 正则推断下一个分片（后备方案）
     */
    private String predictNextUrl(String url, int offset) {
        try {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\d+)(\\.[a-z0-9]+)$");
            java.util.regex.Matcher matcher = pattern.matcher(url);
            if (matcher.find()) {
                int currentNum = Integer.parseInt(matcher.group(1));
                String ext = matcher.group(2);
                return url.substring(0, matcher.start()) + (currentNum + offset) + ext;
            }
        } catch (Exception ignored) {}
        return null;
    }

    private String getShortUrl(String url) {
        if (url == null) return "null";
        int lastSlash = url.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < url.length() - 1) {
            String name = url.substring(lastSlash + 1);
            return name.length() > 30 ? name.substring(0, 27) + "..." : name;
        }
        return url.length() > 30 ? url.substring(0, 27) + "..." : url;
    }

    /**
     * 预加载的分片数据
     */
    private static class PreloadedSegment implements Runnable {
        private final String url;
        private final DataSource.Factory factory;
        public Future<?> future;
        public byte[] data;
        public boolean ready = false;
        public boolean failed = false;

        public PreloadedSegment(String url, DataSource.Factory factory) {
            this.url = url;
            this.factory = factory;
        }

        public boolean isReady() {
            return ready && data != null && !failed;
        }

        public void cancel() {
            if (future != null) {
                future.cancel(true);
            }
        }

        @Override
        public void run() {
            DataSource ds = null;
            try {
                BufferStatusManager.getInstance().onTaskStarted(url);
                ds = factory.createDataSource();
                DataSpec spec = new DataSpec(android.net.Uri.parse(url));
                long length = ds.open(spec);
                
                // 读取全部数据到内存
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int read;
                long totalRead = 0;
                
                while ((read = ds.read(buffer, 0, buffer.length)) != -1) {
                    baos.write(buffer, 0, read);
                    totalRead += read;
                    
                    // 安全检查：防止内存溢出
                    if (totalRead > 10 * 1024 * 1024) { // 10MB 上限
                        Log.w(TAG, "预加载分片过大，取消: " + url);
                        failed = true;
                        BufferStatusManager.getInstance().onTaskFailed(url, "分片过大");
                        return;
                    }
                }
                
                data = baos.toByteArray();
                ready = true;
                BufferStatusManager.getInstance().onTaskCompleted(url, data.length);
                Log.d(TAG, "预加载完成: " + url + ", 大小: " + data.length);
                
            } catch (Exception e) {
                failed = true;
                BufferStatusManager.getInstance().onTaskFailed(url, e.getMessage());
                Log.w(TAG, "预加载失败: " + url + ", 错误: " + e.getMessage());
            } finally {
                if (ds != null) {
                    try { ds.close(); } catch (IOException ignored) {}
                }
            }
        }
    }
}
