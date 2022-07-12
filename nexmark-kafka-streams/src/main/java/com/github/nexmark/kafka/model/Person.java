package com.github.nexmark.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {
    /** Id of person. */
    public long id; // primary key

    /** Extra person properties. */
    public String name;

    public String emailAddress;

    public String creditCard;

    public String city;

    public String state;

    public long dateTime;

    /** Additional arbitrary payload for performance testing. */
    public String extra;

    @JsonCreator
    public Person(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("emailAddress") String emailAddress,
            @JsonProperty("creditCard") String creditCard,
            @JsonProperty("city") String city,
            @JsonProperty("state") String state,
            @JsonProperty("dateTime") long dateTime,
            @JsonProperty("extra") String extra) {
        this.id = id;
        this.name = name;
        this.emailAddress = emailAddress;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", emailAddress='" + emailAddress + '\'' +
                ", creditCard='" + creditCard + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", dateTime=" + dateTime +
                ", extra='" + extra + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Person person = (Person) o;
        return id == person.id
                &&dateTime == person.dateTime
                && name.equals(person.name)
                && emailAddress.equals(person.emailAddress)
                && creditCard.equals(person.creditCard)
                && city.equals(person.city)
                && state.equals(person.state)
                && extra.equals(person.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, emailAddress, creditCard, city, state, dateTime, extra);
    }
    
}
