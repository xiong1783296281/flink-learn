package org.xiong.apitest.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.xiong.apitest.beans.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author: xiong
 * @create at 2020/12/31
 */
public class SinkTest4CustomMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"3ab1d65cca35:9092");

        DataStreamSource<String> inputStream = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");
        DataStream<SensorReading> dataStream = inputStream.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );

        dataStream.addSink(new MyJdbcSink());

        env.execute();


    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading>{

        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_learn","root","123456");
            insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }

}
