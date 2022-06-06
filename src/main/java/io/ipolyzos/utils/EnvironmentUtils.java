package io.ipolyzos.utils;

import static org.apache.flink.configuration.TaskManagerOptions.CPU_CORES;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvironmentUtils {
    public static StreamExecutionEnvironment initEnvWithWebUI(boolean withWebUI){
        Configuration flinkConfig = new Configuration();

//        flinkConfig.set(BIND_PORT, "8082");
        flinkConfig.set(CPU_CORES, 4.0);
        flinkConfig.set(TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
        flinkConfig.set(TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256));
        flinkConfig.set(MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(1024));

        if (withWebUI) {
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

}
