package io.ipolyzos.utils;

import io.ipolyzos.models.Account;
import io.ipolyzos.models.Customer;
import io.ipolyzos.models.Transaction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.stream.Stream;

public class DataSourceUtils {
    public static Stream<String> loadDataFile(String fileName) throws IOException {
        return Files.lines(
                Paths.get(System.getProperty("user.dir") +  fileName)
        ).skip(1);
    }

    public static Transaction toTransaction(String line) {
        String[] tokens = line.split(",");
        return new Transaction(
                tokens[1],
                tokens[2],
                tokens[3],
                tokens[4],
                tokens[5],
                tokens[6],
                tokens[7],
                Timestamp.valueOf(tokens[15].replace("T", " ")).getTime()
        );
    }

    public static Account toAccount(String line) {
        String[] tokens = line.split(",");
        return new Account(
                tokens[0],
                tokens[1],
                tokens[2],
                tokens[7]
        );
    }

    public static Customer toCustomer(String line) {
        String[] tokens = line.split(",");
        return new Customer(
                tokens[0],
                tokens[1],
                tokens[7],
                tokens[8],
                tokens[9],
                tokens[10],
                tokens[11],
                tokens[12],
                tokens[13],
                tokens[14],
                tokens[15],
                tokens[16],
                tokens[17],
                tokens[18],
                tokens[19]
        );
    }

    private static long parseId(String id) {
        return (long) Double.parseDouble(id.replace(".0", ""));
    }
}
