package org.xiong.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;

    private Long timestamp;

    private Double temperature;


}
