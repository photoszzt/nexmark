CREATE stream data_gen (
    type int,
    newPerson STRUCT<
        id BIGINT,
        name VARCHAR,
        emailAddress VARCHAR,
        creditCard  VARCHAR,
        city  VARCHAR,
        state  VARCHAR,
        dateTime BIGINT,
        extra  VARCHAR
    >,
    newAuction STRUCT<
        id BIGINT,
        itemName  VARCHAR,
        description  VARCHAR,
        initialBid  BIGINT,
        reserve  BIGINT,
        dateTime  BIGINT,
        expires  BIGINT,
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
        dateTime  BIGINT,
        extra  VARCHAR
    >
) WITH (
    KAFKA_TOPIC = 'nexmark_src',
    VALUE_FORMAT = 'JSON',
    partitions = 1
);

create stream person as
    select 
        data_gen.newPerson->id as id,
        data_gen.newPerson->name as name,
        data_gen.newPerson->emailAddress as emailAddress,
        data_gen.newPerson->creditCard as creditCard,
        data_gen.newPerson->city as city,
        data_gen.newPerson->state as state,
        data_gen.newPerson->dateTime as dateTime,
        data_gen.newPerson->extra as extra
    FROM data_gen
    where data_gen.type = 0
    emit changes;

create stream auction AS
    SELECT 
        data_gen.newAuction->id as id,
        data_gen.newAuction->itemName as itemName,
        data_gen.newAuction->description as description,
        data_gen.newAuction->initialBid as initialBid,
        data_gen.newAuction->reserve as reserve,
        data_gen.newAuction->dateTime as dateTime,
        data_gen.newAuction->expires as expires,
        data_gen.newAuction->seller as seller,
        data_gen.newAuction->category as category,
        data_gen.newAuction->extra as extra
    FROM data_gen
    where data_gen.type = 1
    emit changes;

create stream bid as
    select 
        data_gen.bid->auction as auction,
        data_gen.bid->bidder as bidder,
        data_gen.bid->price as price,
        data_gen.bid->channel as channel,
        data_gen.bid->url as url,
        data_gen.bid->dateTime as dateTime,
        data_gen.bid->extra as extra
    FROM data_gen
    where data_gen.type = 2
    emit changes;