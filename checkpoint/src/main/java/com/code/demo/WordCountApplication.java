package com.code.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author panjb
 * @date 2021-09-09 22:49
 */
public class WordCountApplication {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //开启checkpoint
        //每 5000ms 开始一次 checkpoint，模式为：EXACTLY_ONCE
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置写入checkpoint快照的存储位置
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node1:9000/checkpoint");
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> source = env.socketTextStream(host, port);
        source.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(k -> k.f0)
                .sum(1).
                print();
        env.execute("Test Checkpoint Job");
    }
}
