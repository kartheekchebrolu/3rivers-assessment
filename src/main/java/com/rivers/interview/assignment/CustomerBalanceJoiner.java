package com.rivers.interview.assignment;

import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.Transaction;
import com.ibm.gbs.schema.CustomerBalance;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class CustomerBalanceJoiner implements ValueJoiner<Transaction, Customer, CustomerBalance> {

    @Override
    public CustomerBalance apply(Transaction transaction, Customer customer) {
        CustomerBalance balance = CustomerBalance.newBuilder().setAccountId(customer.getAccountId()).
                setCustomerId(customer.getCustomerId()).setBalance(transaction.getBalance()).
                setPhoneNumber(customer.getPhoneNumber()).build();
        System.out.println(balance.toString());
        return balance;   }
}
