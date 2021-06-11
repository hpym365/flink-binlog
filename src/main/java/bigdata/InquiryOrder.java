package bigdata;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: Haben
 * @description:
 * @date: 2021-05-30 08:45
 * @version: 1.0
 **/
@Slf4j
@Data
@ToString
public class InquiryOrder {
	private long orderNo;//1
	private String providerAppId;//3
	private String providerId;//4
	private String providerNo;//5
	private String cityCode;//14
	private int inquiryStatus;//39
	private int dispatchType;//47
	private String brandName; //-1
	private String createdAt;//43
	private String updatedAt;//44
}
