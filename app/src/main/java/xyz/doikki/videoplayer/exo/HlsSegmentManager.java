package xyz.doikki.videoplayer.exo;

import android.net.Uri;
import android.util.Log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HLS 分片列表管理器
 * 解析 m3u8 播放列表，提供分片 URL 预测能力
 */
public class HlsSegmentManager {
    private static final String TAG = "HlsSegmentManager";
    
    private static volatile HlsSegmentManager sInstance;
    
    // 缓存已解析的播放列表：baseUrl -> 分片列表
    private final ConcurrentHashMap<String, List<String>> playlistCache = new ConcurrentHashMap<>();
    // 当前分片索引缓存
    private final ConcurrentHashMap<String, Integer> segmentIndexCache = new ConcurrentHashMap<>();
    
    private HlsSegmentManager() {}
    
    public static HlsSegmentManager getInstance() {
        if (sInstance == null) {
            synchronized (HlsSegmentManager.class) {
                if (sInstance == null) {
                    sInstance = new HlsSegmentManager();
                }
            }
        }
        return sInstance;
    }
    
    /**
     * 根据当前分片 URL 获取后续 N 个分片的 URL
     */
    public List<String> getNextSegments(String currentSegmentUrl, int count) {
        List<String> result = new ArrayList<>();
        
        // 查找当前分片所属的播放列表
        for (String baseUrl : playlistCache.keySet()) {
            List<String> segments = playlistCache.get(baseUrl);
            if (segments == null) continue;
            
            int currentIndex = segments.indexOf(currentSegmentUrl);
            if (currentIndex >= 0) {
                // 找到了，获取后续分片
                for (int i = 1; i <= count && (currentIndex + i) < segments.size(); i++) {
                    result.add(segments.get(currentIndex + i));
                }
                segmentIndexCache.put(baseUrl, currentIndex);
                Log.d(TAG, "找到分片索引: " + currentIndex + ", 后续分片数: " + result.size());
                break;
            }
        }
        
        return result;
    }
    
    /**
     * 解析 m3u8 播放列表并缓存分片 URL
     */
    public void parseAndCachePlaylist(String m3u8Url) {
        if (m3u8Url == null || playlistCache.containsKey(m3u8Url)) {
            return;
        }
        
        // 异步解析
        new Thread(() -> {
            try {
                List<String> segments = parseM3u8(m3u8Url);
                if (!segments.isEmpty()) {
                    playlistCache.put(m3u8Url, segments);
                    Log.d(TAG, "已缓存播放列表: " + m3u8Url + ", 分片数: " + segments.size());
                    BufferStatusManager.getInstance().onPreloadScheduled("解析到 " + segments.size() + " 个分片");
                }
            } catch (Exception e) {
                Log.w(TAG, "解析 m3u8 失败: " + m3u8Url, e);
            }
        }).start();
    }
    
    /**
     * 从分片 URL 推断并解析其所属的 m3u8
     */
    public void inferAndParsePlaylist(String segmentUrl) {
        if (segmentUrl == null) return;
        
        // 检查是否已在某个列表中
        for (List<String> segments : playlistCache.values()) {
            if (segments.contains(segmentUrl)) {
                return; // 已缓存
            }
        }
        
        // 尝试推断 m3u8 URL（通常在同一目录下）
        String baseUrl = getBaseUrl(segmentUrl);
        String possibleM3u8 = baseUrl + "index.m3u8";
        
        // 也可能是其他命名
        String[] possibleNames = {"index.m3u8", "playlist.m3u8", "master.m3u8", "live.m3u8"};
        for (String name : possibleNames) {
            String m3u8Url = baseUrl + name;
            if (!playlistCache.containsKey(m3u8Url)) {
                parseAndCachePlaylist(m3u8Url);
            }
        }
    }
    
    private List<String> parseM3u8(String m3u8Url) throws Exception {
        List<String> segments = new ArrayList<>();
        String baseUrl = getBaseUrl(m3u8Url);
        
        HttpURLConnection conn = null;
        try {
            URL url = new URL(m3u8Url);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            
            if (conn.getResponseCode() != 200) {
                return segments;
            }
            
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                // 跳过注释和空行
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
                // 这是一个分片 URL
                String segmentUrl;
                if (line.startsWith("http://") || line.startsWith("https://")) {
                    segmentUrl = line;
                } else if (line.startsWith("/")) {
                    // 绝对路径
                    Uri uri = Uri.parse(m3u8Url);
                    segmentUrl = uri.getScheme() + "://" + uri.getHost() + line;
                } else {
                    // 相对路径
                    segmentUrl = baseUrl + line;
                }
                
                segments.add(segmentUrl);
            }
            
            reader.close();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        
        return segments;
    }
    
    private String getBaseUrl(String url) {
        int lastSlash = url.lastIndexOf('/');
        if (lastSlash > 0) {
            return url.substring(0, lastSlash + 1);
        }
        return url;
    }
    
    /**
     * 清理缓存
     */
    public void clearCache() {
        playlistCache.clear();
        segmentIndexCache.clear();
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("已缓存播放列表: ").append(playlistCache.size()).append("\n");
        for (String key : playlistCache.keySet()) {
            List<String> segments = playlistCache.get(key);
            Integer index = segmentIndexCache.get(key);
            sb.append("  - ").append(getShortUrl(key))
              .append(": ").append(segments != null ? segments.size() : 0).append(" 分片")
              .append(", 当前: ").append(index != null ? index : "?")
              .append("\n");
        }
        return sb.toString();
    }
    
    private String getShortUrl(String url) {
        if (url == null) return "null";
        if (url.length() > 50) {
            return "..." + url.substring(url.length() - 47);
        }
        return url;
    }
}
