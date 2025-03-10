import java.util.Collections;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class DataPoint {
    String name;
    float value;

    public DataPoint(String name, float value) {
        this.name = name;
        this.value = value;
    }

    public String toString() {
        return name + ": " + value;
    }
}

public class TopTen {
    private PriorityQueue<DataPoint> minHeap = new PriorityQueue<>(Comparator.comparingDouble(a -> a.value));

    public void add(DataPoint dataPoint) {
        if (minHeap.size() < 10) {
            minHeap.add(dataPoint);
        } else if (dataPoint.value > minHeap.peek().value) {
            minHeap.poll();
            minHeap.add(dataPoint);
        }
    }

    public List<String> getTopTen() {
        List<DataPoint> topTen = new ArrayList<>(minHeap);
        topTen.sort((a, b) -> Float.compare(b.value, a.value));

        List<String> topTenFormatted = new ArrayList<>();
        for (DataPoint dp : topTen) {
            topTenFormatted.add(dp.toString());
        }

        return topTenFormatted;
    }
}

//public class Main {
//    public static void main(String[] args) {
//        TopTen topTen = new TopTen();
//
//        // 添加数据点
//        topTen.add(new DataPoint("A", 12.5f));
//        topTen.add(new DataPoint("B", 3.2f));
//        // ... 更多数据点
//
//        // 获取并打印最大的10个数据点及其对应字符串
//        List<String> result = topTen.getTopTen();
//        for (String dp : result) {
//            System.out.println(dp);
//        }
//    }
//}
