package xyz.doikki.videoplayer.exo;

import android.util.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 多线程缓冲状态管理器
 * 用于收集和展示缓冲线程的实时状态
 */
public class BufferStatusManager {
    private static final String TAG = "BufferStatusManager";
    
    private static volatile BufferStatusManager sInstance;
    
    // 缓冲状态信息
    private boolean isMultithreadEnabled = false;
    private int threadCount = 0;
    private int activeTaskCount = 0;
    private int preloadHitCount = 0;
    private int preloadMissCount = 0;
    private long totalBytesLoaded = 0;
    private final CopyOnWriteArrayList<String> recentLogs = new CopyOnWriteArrayList<>();
    private static final int MAX_LOG_SIZE = 20;
    
    // 状态变更监听器
    private OnStatusChangeListener listener;
    
    public interface OnStatusChangeListener {
        void onStatusChanged();
    }
    
    private BufferStatusManager() {}
    
    public static BufferStatusManager getInstance() {
        if (sInstance == null) {
            synchronized (BufferStatusManager.class) {
                if (sInstance == null) {
                    sInstance = new BufferStatusManager();
                }
            }
        }
        return sInstance;
    }
    
    public void setListener(OnStatusChangeListener listener) {
        this.listener = listener;
    }
    
    public void setMultithreadEnabled(boolean enabled, int threads) {
        this.isMultithreadEnabled = enabled;
        this.threadCount = threads;
        addLog("多线程缓冲: " + (enabled ? "开启 (" + threads + " 线程)" : "关闭"));
        notifyChange();
    }
    
    public void onTaskStarted(String uri) {
        activeTaskCount++;
        addLog("开始加载: " + getShortUri(uri));
        notifyChange();
    }
    
    public void onTaskCompleted(String uri, long bytes) {
        activeTaskCount = Math.max(0, activeTaskCount - 1);
        totalBytesLoaded += bytes;
        addLog("完成加载: " + getShortUri(uri) + " (" + formatBytes(bytes) + ")");
        notifyChange();
    }
    
    public void onTaskFailed(String uri, String error) {
        activeTaskCount = Math.max(0, activeTaskCount - 1);
        addLog("加载失败: " + getShortUri(uri) + " - " + error);
        notifyChange();
    }
    
    public void onPreloadHit(String uri) {
        preloadHitCount++;
        addLog("预加载命中: " + getShortUri(uri));
        notifyChange();
    }
    
    public void onPreloadMiss(String uri) {
        preloadMissCount++;
        addLog("预加载未命中: " + getShortUri(uri));
        notifyChange();
    }
    
    public void onPreloadScheduled(String uri) {
        addLog("调度预加载: " + getShortUri(uri));
        notifyChange();
    }
    
    public void reset() {
        activeTaskCount = 0;
        preloadHitCount = 0;
        preloadMissCount = 0;
        totalBytesLoaded = 0;
        recentLogs.clear();
        addLog("状态已重置");
        notifyChange();
    }
    
    // Getters
    public boolean isMultithreadEnabled() { return isMultithreadEnabled; }
    public int getThreadCount() { return threadCount; }
    public int getActiveTaskCount() { return activeTaskCount; }
    public int getPreloadHitCount() { return preloadHitCount; }
    public int getPreloadMissCount() { return preloadMissCount; }
    public long getTotalBytesLoaded() { return totalBytesLoaded; }
    
    public String getStatusSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("【多线程缓冲状态】\n");
        sb.append("状态: ").append(isMultithreadEnabled ? "已开启" : "已关闭").append("\n");
        if (isMultithreadEnabled) {
            sb.append("线程数: ").append(threadCount).append("\n");
            sb.append("活跃任务: ").append(activeTaskCount).append("\n");
            sb.append("预加载命中: ").append(preloadHitCount).append("\n");
            sb.append("预加载未命中: ").append(preloadMissCount).append("\n");
            sb.append("已加载: ").append(formatBytes(totalBytesLoaded)).append("\n");
            
            int total = preloadHitCount + preloadMissCount;
            if (total > 0) {
                int hitRate = (preloadHitCount * 100) / total;
                sb.append("命中率: ").append(hitRate).append("%\n");
            }
        }
        return sb.toString();
    }
    
    public List<String> getRecentLogs() {
        return new ArrayList<>(recentLogs);
    }
    
    private void addLog(String log) {
        String timestamp = java.text.SimpleDateFormat.getTimeInstance().format(new java.util.Date());
        String logEntry = "[" + timestamp + "] " + log;
        recentLogs.add(logEntry);
        while (recentLogs.size() > MAX_LOG_SIZE) {
            recentLogs.remove(0);
        }
        Log.d(TAG, log);
    }
    
    private void notifyChange() {
        if (listener != null) {
            listener.onStatusChanged();
        }
    }
    
    private String getShortUri(String uri) {
        if (uri == null) return "null";
        int lastSlash = uri.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < uri.length() - 1) {
            String name = uri.substring(lastSlash + 1);
            if (name.length() > 30) {
                return name.substring(0, 27) + "...";
            }
            return name;
        }
        return uri.length() > 30 ? uri.substring(0, 27) + "..." : uri;
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    }
}
