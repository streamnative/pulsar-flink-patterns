package io.ipolyzos.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
    private String transactionId;
    private String accountId;
    private String type;
    private String operation;
    private String amount;
    private String balance;
    private String kSymbol;
    private Long eventTime;
}
