package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    private String clientId;
    private String sex;
    private String social;
    private String first;
    private String middle;
    private String last;
    private String phone;
    private String email;
    private String address1;
    private String address2;
    private String city;
    private String state;
    private String zipcode;
    private String districtId;
    private String birthData;
}
