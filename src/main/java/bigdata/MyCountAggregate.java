package bigdata;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MyCountAggregate<T1> implements AggregateFunction<T1, Long, Long> {

    /**
     * 以一个窗口数据为一批次
     * 一个批次中的数据出现一个新的key都会调用这个方法,用于初始化key的默认值
     *
     * @return
     */
    @Override
    public Long createAccumulator() { //初始化值数值调用
        return 0L;
    }

    /**
     * 以一个窗口数据为一批次
     * 每一个key都会调用这个方法,相同的key调用这个方法中的t2属于同一个引用,这个t2通常用于记录累计次数
     *
     * @param t1 传入的key
     * @param t2 相同key的引用
     * @return
     */
    @Override
    public Long add(T1 t1, Long t2) {
        t2 = t2 + 1;
        return t2;
    }

    /**
     * 以一个窗口数据为一批次
     * 输出一格窗口中的某一类key合并后(相同的key都调用了add之后)产生的结果
     *
     * @param t
     * @return
     */
    @Override
    public Long getResult(Long t) {
        System.out.println("getResult->" + t);
        return t;
    }

    /**
     * 以一个窗口数据为一批次
     * Flink是分布式处理数据的,一个窗口的数据可能分布在多个服务器中,merge相当于是把不同的服务器的数据进行合并
     *
     * @param t1
     * @param t2
     * @return
     */
    @Override
    public Long merge(Long t1, Long t2) {
        System.out.println("merge->" + t1 + t2);
        return t1 + t2;
    }
}