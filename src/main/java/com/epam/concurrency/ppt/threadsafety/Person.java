package com.epam.concurrency.ppt.threadsafety;


public class Person {

    private Address address;

    public Person(Address address) {
        this.address = address;
    }

    public Address getAddress() {
        return this.address;
    }
}
