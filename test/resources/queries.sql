
create temp table test(
    f_01 int2,
    f_02 int4,
    f_03 int8,
    f_04 boolean,
    f_05 float4,
    f_06 float8,
    f_07 text,
    f_08 varchar(12),
    f_09 time,
    f_10 timetz,
    f_11 date,
    f_12 timestamp,
    f_13 timestamptz,
    f_14 bytea,
    f_15 json,
    f_16 jsonb,
    f_17 uuid,
    f_18 numeric(12,3),
    f_19 text null
);

insert into test values (
    1, 2, 3,
    true,
    123.456, 654.321,
    'hello', 'world',
    '10:42:35', '10:42:35+0030',
    '2025-11-30',
    '2025-11-30 10:42:35', '2025-11-30 10:42:35.123567+0030',
    '\xDEADBEEF',
    '{"foo": [1, 2, 3, {"kek": [true, false, null]}]}',
    '{"foo": [1, 2, 3, {"kek": [true, false, null]}]}',
    '4bda6037-1c37-4051-9898-13b82f1bd712',
    '123456.123456',
    null
);

\copy test to '/Users/ivan/Downloads/dump.bin' with (format binary);
