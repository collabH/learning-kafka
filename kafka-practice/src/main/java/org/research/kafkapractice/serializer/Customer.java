package org.research.kafkapractice.serializer;

import lombok.Data;

/**
 * @fileName: Customer.java
 * @description: kafka消费数据体
 * @author: by echo huang
 * @date: 2020-04-24 16:00
 */
@Data
public class Customer {
    private Integer id;
    private String name;
}
