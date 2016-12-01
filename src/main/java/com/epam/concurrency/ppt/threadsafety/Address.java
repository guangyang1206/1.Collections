package com.epam.concurrency.ppt.threadsafety;


public class Address {

    private String houseNo;
    private String streetNo;
    private String pin;

    public Address(String houseNo, String streetNo, String pin) {
        this.houseNo = houseNo;
        this.streetNo = streetNo;
        this.pin = pin;
    }

    public String getHouseNo() {
        return houseNo;
    }

    public void setHouseNo(String houseNo) {
        this.houseNo = houseNo;
    }

    public String getStreetNo() {
        return streetNo;
    }

    public void setStreetNo(String streetNo) {
        this.streetNo = streetNo;
    }

    public String getPin() {
        return pin;
    }

    public void setPin(String pin) {
        this.pin = pin;
    }
}
