CREATE TEMP FUNCTION
  udf_zeroed_365_days_bytes() AS (
    CONCAT(
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00\x00\x00',
        b'\x00\x00'));
