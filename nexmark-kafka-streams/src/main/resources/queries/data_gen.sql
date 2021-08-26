CREATE stream data_gen (
    event_type int,
    person STRUCT<
        id BIGINT,
        name VARCHAR,
        emailAddress VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        dateTime VARCHAR,
        extra  VARCHAR
    >,
    auction STRUCT<
        id BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        dateTime  VARCHAR,
        expires  TIMESTAMP(3),
        seller  BIGINT,
        category  BIGINT,
        extra  VARCHAR
    >,
    bid STRUCT<
        auction  BIGINT,
        bidder  BIGINT,
        price  BIGINT,
        channel  VARCHAR,
        url  VARCHAR,
        dateTime  VARCHAR,
        extra  VARCHAR
    >
) WITH (
    KAFKA_TOPIC = 'nexmark',
    VALUE_FORMAT = 'JSON',
    partitions = 1
)
