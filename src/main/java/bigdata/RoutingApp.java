package bigdata;


import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

@Slf4j
public class RoutingApp {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> data = env.socketTextStream("localhost", 9999, "\n");

		SingleOutputStreamOperator<InquiryOrder> map = data.map((MapFunction<String, InquiryOrder>) RoutingApp::convert);

		map.print();

		// 5. submit job
		env.execute("cdc-routing-application");
	}

	public static InquiryOrder convert(String message) {
		message = "{\"data\":[{\"id\":\"9278394\",\"inquiry_order_no\":\"321872393906098176\",\"order_no\":\"321872393906098176\",\"provider_app_id\":\"1027\",\"provider_id\":\"1\",\"provider_no\":\"174986925763985408\",\"provider_name\":\"滴滴\",\"provider_order_no\":null,\"start_addr\":null,\"start_lon\":\"120.56277\",\"start_lat\":\"27.61083\",\"end_addr\":null,\"end_lon\":\"120.564605\",\"end_lat\":\"27.605042\",\"city_code\":\"0577\",\"city_name\":\"温州市\",\"business_type\":\"1\",\"business_type_show\":\"快车\",\"capacity_type\":\"1\",\"capacity_type_show\":\"经济\",\"fare_category\":\"1\",\"fare_category_show\":\"工作日\",\"driver_passenger_distance\":null,\"driver_passenger_time\":null,\"plan_use_time\":\"122\",\"plan_mileage\":\"966\",\"plan_total_amount\":\"1080\",\"plan_start_fare\":\"1080\",\"plan_long_distance\":\"0\",\"plan_long_distance_fare\":\"0\",\"plan_peak_time_fare\":\"0\",\"plan_other_peak_time_fare\":\"0\",\"plan_mileage_fare\":\"0\",\"plan_time_fare\":\"0\",\"plan_other_fare\":\"0\",\"fare_rule_id\":\"5909\",\"channel\":\"didi\",\"deduct_rule_id\":null,\"hands_up\":\"0\",\"inquiry_status\":\"0\",\"failed_reason\":null,\"failed_reason_show\":null,\"price_type\":\"0\",\"created_at\":\"2021-06-07 04:45:59\",\"updated_at\":\"2021-06-07 04:45:59\",\"batch_no\":\"321872394145173505\",\"plan_map_type\":\"3\",\"dispatch_type\":\"0\",\"multi_status\":null}],\"database\":\"mars_inquiry_order\",\"es\":1623012359000,\"id\":100873019,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\",\"inquiry_order_no\":\"bigint(20)\",\"order_no\":\"bigint(20)\",\"provider_app_id\":\"varchar(10)\",\"provider_id\":\"int(11)\",\"provider_no\":\"varchar(50)\",\"provider_name\":\"varchar(50)\",\"provider_order_no\":\"varchar(500)\",\"start_addr\":\"varchar(255)\",\"start_lon\":\"double(10,6)\",\"start_lat\":\"double(10,6)\",\"end_addr\":\"varchar(255)\",\"end_lon\":\"double(10,6)\",\"end_lat\":\"double(10,6)\",\"city_code\":\"varchar(45)\",\"city_name\":\"varchar(45)\",\"business_type\":\"varchar(20)\",\"business_type_show\":\"varchar(20)\",\"capacity_type\":\"varchar(20)\",\"capacity_type_show\":\"varchar(20)\",\"fare_category\":\"int(2)\",\"fare_category_show\":\"varchar(20)\",\"driver_passenger_distance\":\"int(11)\",\"driver_passenger_time\":\"int(11)\",\"plan_use_time\":\"int(11)\",\"plan_mileage\":\"int(11)\",\"plan_total_amount\":\"int(11)\",\"plan_start_fare\":\"int(11)\",\"plan_long_distance\":\"int(11)\",\"plan_long_distance_fare\":\"int(11)\",\"plan_peak_time_fare\":\"int(11)\",\"plan_other_peak_time_fare\":\"int(11)\",\"plan_mileage_fare\":\"int(11)\",\"plan_time_fare\":\"int(11)\",\"plan_other_fare\":\"int(11)\",\"fare_rule_id\":\"int(11)\",\"channel\":\"varchar(40)\",\"deduct_rule_id\":\"int(11)\",\"hands_up\":\"int(2)\",\"inquiry_status\":\"tinyint(2)\",\"failed_reason\":\"int(2)\",\"failed_reason_show\":\"varchar(100)\",\"price_type\":\"tinyint(2)\",\"created_at\":\"timestamp\",\"updated_at\":\"timestamp\",\"batch_no\":\"bigint(20)\",\"plan_map_type\":\"tinyint(2)\",\"dispatch_type\":\"tinyint(2)\",\"multi_status\":\"bigint(20)\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"inquiry_order_no\":-5,\"order_no\":-5,\"provider_app_id\":12,\"provider_id\":4,\"provider_no\":12,\"provider_name\":12,\"provider_order_no\":12,\"start_addr\":12,\"start_lon\":8,\"start_lat\":8,\"end_addr\":12,\"end_lon\":8,\"end_lat\":8,\"city_code\":12,\"city_name\":12,\"business_type\":12,\"business_type_show\":12,\"capacity_type\":12,\"capacity_type_show\":12,\"fare_category\":4,\"fare_category_show\":12,\"driver_passenger_distance\":4,\"driver_passenger_time\":4,\"plan_use_time\":4,\"plan_mileage\":4,\"plan_total_amount\":4,\"plan_start_fare\":4,\"plan_long_distance\":4,\"plan_long_distance_fare\":4,\"plan_peak_time_fare\":4,\"plan_other_peak_time_fare\":4,\"plan_mileage_fare\":4,\"plan_time_fare\":4,\"plan_other_fare\":4,\"fare_rule_id\":4,\"channel\":12,\"deduct_rule_id\":4,\"hands_up\":4,\"inquiry_status\":-6,\"failed_reason\":4,\"failed_reason_show\":12,\"price_type\":-6,\"created_at\":93,\"updated_at\":93,\"batch_no\":-5,\"plan_map_type\":-6,\"dispatch_type\":-6,\"multi_status\":-5},\"table\":\"inquiry_order_202106_7\",\"ts\":1623012359198,\"type\":\"INSERT\"}";
		return JSON.parseObject(message).getJSONArray("data").getObject(0, InquiryOrder.class);
	}

}
