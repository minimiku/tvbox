package xyz.doikki.videoplayer.exo;

import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.media3.datasource.DataSource;
import androidx.media3.datasource.DataSpec;
import androidx.media3.datasource.TransferListener;

import com.github.tvbox.osc.util.HawkUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多线程并发数据源
 * 
 * 缓存策略：只保留前方分片，播放完立即释放
 * - 播放完成的分片立即从缓存移除
 * - 只预加载当前位置之后的分片
 * - 快进/Seek 时清理不相关的缓存
 */
public class ConcurrentDataSource implements DataSource {

    private static final String TAG = "ConcurrentDataSource";
    private static final int DEFAULT_PRELOAD_COUNT = 2;
    private static final int MAX_CACHE_SIZE = 5; // 最多缓存5个前方分片
    private static final long MAX_SEGMENT_SIZE = 5 * 1024 * 1024;
    private static final long PRELOAD_INTERVAL_MS = 300;
    
    // 全局静态资源
    private static volatile ExecutorService sSharedExecutor;
    private static final Object sExecutorLock = new Object();
    private static final ConcurrentHashMap<String, PreloadedSegment> sPreloadCache = new ConcurrentHashMap<>();
    private static volatile long sLastPreloadTime = 0;
    private static final AtomicInteger sActivePreloads = new AtomicInteger(0);
    
    // 记录播放顺序，用于智能淘汰
    private static final List<String> sPlayedSegments = new ArrayList<>();
    private static volatile String sCurrentSegmentUrl = null;

    private final DataSource.Factory upstreamFactory;
    private final int threadCount;
    private final int preloadCount;

    private DataSource currentDataSource;
    private android.net.Uri currentUri;
    private PreloadedSegment currentPreloaded;
    private int currentReadPosition;

    public ConcurrentDataSource(DataSource.Factory upstreamFactory) {
        this.upstreamFactory = upstreamFactory;
        this.threadCount = Math.min(HawkUtils.getExoBufferThreadCount(), 6);
        this.preloadCount = Math.min(threadCount, DEFAULT_PRELOAD_COUNT);
        initExecutor();
        BufferStatusManager.getInstance().setMultithreadEnabled(true, this.threadCount);
    }

    private void initExecutor() {
        synchronized (sExecutorLock) {
            if (sSharedExecutor == null || sSharedExecutor.isShutdown()) {
                sSharedExecutor = Executors.newFixedThreadPool(4);
            }
        }
    }

    @Override
    public void addTransferListener(@NonNull TransferListener transferListener) {}

    @Override
    public long open(@NonNull DataSpec dataSpec) throws IOException {
        this.currentUri = dataSpec.uri;
        String uriString = currentUri.toString();
        this.currentReadPosition = 0;
        this.currentPreloaded = null;

        // 记录当前播放的分片，用于智能淘汰
        String previousSegment = sCurrentSegmentUrl;
        sCurrentSegmentUrl = uriString;
        
        // 将上一个播放完的分片加入已播放列表
        if (previousSegment != null && !previousSegment.equals(uriString)) {
            synchronized (sPlayedSegments) {
                if (!sPlayedSegments.contains(previousSegment)) {
                    sPlayedSegments.add(previousSegment);
                }
            }
            // 立即释放已播放的分片缓存
            releasePlayedSegments();
        }

        // 1. 检查缓存
        PreloadedSegment preloaded = sPreloadCache.remove(uriString);
        if (preloaded != null && preloaded.isReady()) {
            Log.d(TAG, "命中预加载: " + getShortUrl(uriString));
            BufferStatusManager.getInstance().onPreloadHit(uriString);
            this.currentPreloaded = preloaded;
            scheduleNextPreloads(uriString);
            return preloaded.data.length;
        }
        
        BufferStatusManager.getInstance().onPreloadMiss(uriString);

        // 2. 智能清理：只保留前方分片
        cleanupNonForwardCache(uriString);

        // 3. 正常打开
        currentDataSource = upstreamFactory.createDataSource();
        long length = currentDataSource.open(dataSpec);

        // 4. 预加载前方分片
        scheduleNextPreloads(uriString);

        return length;
    }

    @Override
    public int read(@NonNull byte[] buffer, int offset, int length) throws IOException {
        if (currentPreloaded != null) {
            byte[] data = currentPreloaded.data;
            int remaining = data.length - currentReadPosition;
            if (remaining <= 0) {
                return -1;
            }
            int toRead = Math.min(length, remaining);
            System.arraycopy(data, currentReadPosition, buffer, offset, toRead);
            currentReadPosition += toRead;
            return toRead;
        }
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
        // 关闭时释放当前预加载数据的内存
        if (currentPreloaded != null) {
            currentPreloaded.releaseData();
            currentPreloaded = null;
        }
        currentReadPosition = 0;
        if (currentDataSource != null) {
            currentDataSource.close();
            currentDataSource = null;
        }
    }

    /**
     * 释放已播放的分片缓存
     */
    private void releasePlayedSegments() {
        synchronized (sPlayedSegments) {
            for (String url : sPlayedSegments) {
                PreloadedSegment segment = sPreloadCache.remove(url);
                if (segment != null) {
                    segment.cancel();
                    Log.d(TAG, "释放已播放分片: " + getShortUrl(url));
                }
            }
            // 只保留最近 3 个播放记录用于检测
            while (sPlayedSegments.size() > 3) {
                sPlayedSegments.remove(0);
            }
        }
    }

    /**
     * 智能清理：只保留当前位置之后的分片
     */
    private void cleanupNonForwardCache(String currentUrl) {
        // 获取预期的前方分片列表
        List<String> forwardSegments = HlsSegmentManager.getInstance().getNextSegments(currentUrl, MAX_CACHE_SIZE);
        
        // 如果没有分片信息，使用正则预测
        if (forwardSegments.isEmpty()) {
            for (int i = 1; i <= MAX_CACHE_SIZE; i++) {
                String next = predictNextUrl(currentUrl, i);
                if (next != null) forwardSegments.add(next);
            }
        }

        // 移除不在前方列表中的缓存
        Iterator<Map.Entry<String, PreloadedSegment>> it = sPreloadCache.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PreloadedSegment> entry = it.next();
            String url = entry.getKey();
            if (!url.equals(currentUrl) && !forwardSegments.contains(url)) {
                entry.getValue().cancel();
                it.remove();
                Log.d(TAG, "清理非前方分片: " + getShortUrl(url));
            }
        }
    }

    /**
     * 清理全部缓存（切换视频时调用）
     */
    public static void clearAllCache() {
        Log.d(TAG, "清理所有缓存");
        for (PreloadedSegment segment : sPreloadCache.values()) {
            segment.cancel();
        }
        sPreloadCache.clear();
        synchronized (sPlayedSegments) {
            sPlayedSegments.clear();
        }
        sCurrentSegmentUrl = null;
        HlsSegmentManager.getInstance().clearCache();
        BufferStatusManager.getInstance().reset();
    }

    private void scheduleNextPreloads(String currentUrl) {
        if (!HawkUtils.getExoBufferMultithread()) return;

        long now = System.currentTimeMillis();
        if (now - sLastPreloadTime < PRELOAD_INTERVAL_MS) {
            return;
        }
        sLastPreloadTime = now;

        if (sActivePreloads.get() >= threadCount) {
            return;
        }

        // 只预加载前方分片
        List<String> nextSegments = HlsSegmentManager.getInstance().getNextSegments(currentUrl, preloadCount);
        
        if (nextSegments.isEmpty()) {
            for (int i = 1; i <= preloadCount; i++) {
                String nextUrl = predictNextUrl(currentUrl, i);
                if (nextUrl != null && !sPreloadCache.containsKey(nextUrl) && !isPlayed(nextUrl)) {
                    startPreload(nextUrl);
                }
            }
        } else {
            for (String nextUrl : nextSegments) {
                if (!sPreloadCache.containsKey(nextUrl) && !isPlayed(nextUrl)) {
                    startPreload(nextUrl);
                }
            }
        }
    }
    
    private boolean isPlayed(String url) {
        synchronized (sPlayedSegments) {
            return sPlayedSegments.contains(url);
        }
    }

    private void startPreload(String url) {
        if (sPreloadCache.size() >= MAX_CACHE_SIZE) {
            return;
        }
        if (sActivePreloads.get() >= threadCount) {
            return;
        }

        PreloadedSegment segment = new PreloadedSegment(url, upstreamFactory);
        sPreloadCache.put(url, segment);
        sActivePreloads.incrementAndGet();
        
        segment.future = sSharedExecutor.submit(() -> {
            try {
                segment.run();
            } finally {
                sActivePreloads.decrementAndGet();
            }
        });
        
        BufferStatusManager.getInstance().onPreloadScheduled(url);
    }

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
            return name.length() > 20 ? name.substring(0, 17) + "..." : name;
        }
        return url;
    }

    private static class PreloadedSegment implements Runnable {
        private final String url;
        private final DataSource.Factory factory;
        public Future<?> future;
        public byte[] data;
        public volatile boolean ready = false;
        public volatile boolean failed = false;

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
            releaseData();
        }
        
        public void releaseData() {
            data = null; // 释放内存
        }

        @Override
        public void run() {
            DataSource ds = null;
            try {
                BufferStatusManager.getInstance().onTaskStarted(url);
                ds = factory.createDataSource();
                DataSpec spec = new DataSpec(android.net.Uri.parse(url));
                long length = ds.open(spec);
                
                if (length > MAX_SEGMENT_SIZE) {
                    failed = true;
                    BufferStatusManager.getInstance().onTaskFailed(url, "分片过大");
                    return;
                }
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int read;
                long totalRead = 0;
                
                while ((read = ds.read(buffer, 0, buffer.length)) != -1) {
                    baos.write(buffer, 0, read);
                    totalRead += read;
                    
                    if (totalRead > MAX_SEGMENT_SIZE) {
                        failed = true;
                        BufferStatusManager.getInstance().onTaskFailed(url, "超限");
                        return;
                    }
                }
                
                data = baos.toByteArray();
                ready = true;
                BufferStatusManager.getInstance().onTaskCompleted(url, data.length);
                
            } catch (Exception e) {
                failed = true;
                BufferStatusManager.getInstance().onTaskFailed(url, e.getMessage());
            } finally {
                if (ds != null) {
                    try { ds.close(); } catch (IOException ignored) {}
                }
            }
        }
    }
}
