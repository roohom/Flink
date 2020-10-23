package me.iroohom.customizeSource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: Order
 * @Author: Roohom
 * @Function: 订单Bean
 * @Date: 2020/10/22 10:10
 * @Software: IntelliJ IDEA
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String orderId;
    private Integer userId;
    private Integer orderPrice;
    private Long timeStamp;
}
