package com.github.nexmark.kafka.model;

import java.io.Serializable;
import java.time.Instant;
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

    public Instant dateTime;

    /** Additional arbitrary payload for performance testing. */
    public String extra;

    public Person(
            long id,
            String name,
            String emailAddress,
            String creditCard,
            String city,
            String state,
            Instant dateTime,
            String extra) {
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
                && Objects.equals(dateTime, person.dateTime)
                && Objects.equals(name, person.name)
                && Objects.equals(emailAddress, person.emailAddress)
                && Objects.equals(creditCard, person.creditCard)
                && Objects.equals(city, person.city)
                && Objects.equals(state, person.state)
                && Objects.equals(extra, person.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, emailAddress, creditCard, city, state, dateTime, extra);
    }
    
}
