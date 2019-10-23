SELECT
  DATE(submission_timestamp) as submission_date,
  environment.system.os.version AS version,
  environment.system.os.windows_build_number AS build_number,
  environment.system.os.windows_ubr AS ubr,
IF
  ((environment.system.os.windows_build_number <= 10240),
    '1507',
  IF
    ((environment.system.os.windows_build_number <= 10586),
      '1511',
    IF
      ((environment.system.os.windows_build_number <= 14393),
        '1607',
      IF
        ((environment.system.os.windows_build_number <= 15063),
          '1703',
        IF
          ((environment.system.os.windows_build_number <= 16299),
            '1709',
          IF
            ((environment.system.os.windows_build_number <= 17134),
              '1803',
            IF
              ((environment.system.os.windows_build_number <= 17763),
                '1809',
              IF
                ((environment.system.os.windows_build_number <= 18362),
                  '1903',
                IF
                  ((environment.system.os.windows_build_number > 18362),
                    'Insider',
                    NULL))))))))) build_group,
  SPLIT(environment.build.version, ".")[
OFFSET
  (0)] AS ff_build_version,
  normalized_channel
FROM
  telemetry.main
WHERE
  DATE(submission_timestamp) = @submission_date
  AND environment.system.os.name = 'Windows_NT'
  AND environment.system.os.version LIKE '10%'
  AND sample_id=42;
