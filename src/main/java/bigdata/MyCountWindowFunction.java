package bigdata;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyCountWindowFunction implements WindowFunction<Long, String, String, TimeWindow> {
 
    @Override
    public void apply(String value, TimeWindow window, Iterable<Long> input, Collector<String> out) throws Exception {
        out.collect("窗口时间：" + window.getEnd());
        out.collect("key: " + value + "  累计: " + input.iterator().next());//
    }
}