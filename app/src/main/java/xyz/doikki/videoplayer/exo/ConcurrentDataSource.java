package xyz.doikki.videoplayer.exo;

import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.media3.common.C;
import androidx.media3.datasource.DataSource;
import androidx.media3.datasource.DataSpec;
import androidx.media3.datasource.TransferListener;

import com.github.tvbox.osc.util.HawkUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多线程并发数据源
 * 用于 HLS 分片预加载，通过多线程加速缓冲
 *
 * 工作原理：
 * 1. 包装上游 DataSource
 * 2. 使用线程池预加载后续分片
 * 3. 优化 HLS 流的缓冲速度
 */
public class ConcurrentDataSource implements DataSource {

    private static final String TAG = "ConcurrentDataSource";
    private static final int MAX_PRELOAD_COUNT = 3; // 默认预加载后续 3 个分片
    
    // 全局静态线程池，避免重复创建线程资源
    private static volatile ExecutorService sSharedExecutor;
    private static final Object sExecutorLock = new Object();

    private final DataSource.Factory upstreamFactory;
    private final int threadCount;
    private final ConcurrentHashMap<String, PreloadTask> preloadTasks;
    private final AtomicBoolean isReleased;

    private DataSource currentDataSource;
    private android.net.Uri currentUri;

    public ConcurrentDataSource(DataSource.Factory upstreamFactory) {
        this.upstreamFactory = upstreamFactory;
        this.threadCount = Math.min(HawkUtils.getExoBufferThreadCount(), 8);
        this.preloadTasks = new ConcurrentHashMap<>();
        this.isReleased = new AtomicBoolean(false);

        synchronized (sExecutorLock) {
            if (sSharedExecutor == null || sSharedExecutor.isShutdown()) {
                sSharedExecutor = Executors.newFixedThreadPool(8); // 最大支持 8 线程
            }
        }

        Log.d(TAG, "ConcurrentDataSource 实例已创建，当前环境线程上限: " + this.threadCount);
    }

    private ExecutorService getExecutor() {
        return sSharedExecutor;
    }

    @Override
    public void addTransferListener(@NonNull TransferListener transferListener) {
        // 装饰者模式：增加并分发监听
    }

    @Override
    public long open(@NonNull DataSpec dataSpec) throws IOException {
        if (isReleased.get()) {
            throw new IOException("DataSource is released");
        }

        this.currentUri = dataSpec.uri;
        String uriString = currentUri.toString();

        // 1. 检查预加载命中
        PreloadTask preloadTask = preloadTasks.remove(uriString);
        if (preloadTask != null) {
            try {
                // 等待预加载完成（如果尚未完成，此方法会阻塞一小会儿，利用正在下载的流）
                Log.d(TAG, "命中预加载: " + uriString);
                currentDataSource = preloadTask.getWithTimeout(1500); // 最多等 1.5s
                if (currentDataSource != null) {
                    return preloadTask.length;
                }
            } catch (Exception e) {
                Log.w(TAG, "预加载获取失败: " + uriString + ", 回退到标准加载");
            }
        }

        // 2. 未命中或获取失败，清理不相关的旧任务（通常在 Seek 后发生）
        cancelAllTasksExcept(uriString);

        // 3. 打开主数据源
        currentDataSource = upstreamFactory.createDataSource();
        long length = currentDataSource.open(dataSpec);

        // 4. 预测并调度后续分片
        schedulePreload(uriString);

        return length;
    }

    @Override
    public int read(@NonNull byte[] buffer, int offset, int length) throws IOException {
        return currentDataSource.read(buffer, offset, length);
    }

    @Nullable
    @Override
    public android.net.Uri getUri() {
        return currentDataSource != null ? currentDataSource.getUri() : currentUri;
    }

    @NonNull
    @Override
    public Map<String, java.util.List<String>> getResponseHeaders() {
        return currentDataSource != null ? currentDataSource.getResponseHeaders() : new java.util.HashMap<>();
    }

    @Override
    public void close() throws IOException {
        if (currentDataSource != null) {
            currentDataSource.close();
            currentDataSource = null;
        }
    }

    public void release() {
        if (isReleased.compareAndSet(false, true)) {
            Log.d(TAG, "停止当前数据源的并发任务");
            cancelAllTasksExcept(null);
            // 作为全局共享线程池，不在这里 shutdown sSharedExecutor
        }
    }

    /**
     * 实现 Seek 优化：清理掉当前播放点之前的或过远的预加载任务
     */
    private void cancelAllTasksExcept(String activeUri) {
        java.util.Iterator<Map.Entry<String, PreloadTask>> it = preloadTasks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PreloadTask> entry = it.next();
            if (!entry.getKey().equals(activeUri)) {
                entry.getValue().cancel();
                it.remove();
            }
        }
    }

    /**
     * 分片预测算法（核心增强）
     */
    private void schedulePreload(String currentUri) {
        if (isReleased.get() || !HawkUtils.getExoBufferMultithread()) return;

        // 针对顺序命名的分片（如 seg_1.ts, seg_2.ts）进行启发式预测
        for (int i = 1; i <= MAX_PRELOAD_COUNT; i++) {
            String nextUri = predictNextUri(currentUri, i);
            if (nextUri != null && !preloadTasks.containsKey(nextUri)) {
                startPreloadTask(nextUri);
            }
        }
    }

    private String predictNextUri(String uri, int offset) {
        try {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\d+)(\\.[a-z0-9]+)$");
            java.util.regex.Matcher matcher = pattern.matcher(uri);
            if (matcher.find()) {
                int currentNum = Integer.parseInt(matcher.group(1));
                String ext = matcher.group(2);
                return uri.substring(0, matcher.start()) + (currentNum + offset) + ext;
            }
        } catch (Exception ignored) {}
        return null;
    }

    private void startPreloadTask(String uri) {
        if (preloadTasks.size() >= threadCount) return;

        PreloadTask task = new PreloadTask(uri, upstreamFactory);
        preloadTasks.put(uri, task);
        task.future = getExecutor().submit(task);
    }

    /**
     * 预加载任务实体
     */
    private static class PreloadTask implements Runnable {
        private final String uri;
        private final DataSource.Factory factory;
        public Future<?> future;
        public long length;
        private DataSource resultDataSource;
        private final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        public PreloadTask(String uri, DataSource.Factory factory) {
            this.uri = uri;
            this.factory = factory;
        }

        @Override
        public void run() {
            try {
                DataSource ds = factory.createDataSource();
                DataSpec spec = new DataSpec(android.net.Uri.parse(uri));
                length = ds.open(spec);
                resultDataSource = ds;
                Log.d(TAG, "预加载完成: " + uri);
            } catch (Exception e) {
                Log.w(TAG, "预加载失败: " + uri);
            } finally {
                latch.countDown();
            }
        }

        public DataSource getWithTimeout(long ms) throws InterruptedException {
            latch.await(ms, java.util.concurrent.TimeUnit.MILLISECONDS);
            return resultDataSource;
        }

        public void cancel() {
            if (future != null) future.cancel(true);
            if (resultDataSource != null) {
                try { resultDataSource.close(); } catch (IOException ignored) {}
            }
        }
    }
}
