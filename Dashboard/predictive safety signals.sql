CREATE TABLE wearable_vitals (
    patient_id TEXT,
    timestamp TIMESTAMP,
    hr FLOAT,
    hrv FLOAT,
    device_on INT
);

--activity_log
CREATE TABLE activity_log (
    patient_id TEXT,
    timestamp TIMESTAMP,
    activity_type TEXT,
    steps INT,
    motion_flag TEXT
);
--ae_reports
CREATE TABLE ae_reports (
    patient_id TEXT,
    ae_timestamp TIMESTAMP,
    ae_term TEXT,
    seriousness INT
);

SELECT COUNT(*)FROM activity_log;
SELECT COUNT(*)FROM ae_reports ;
SELECT COUNT(*)FROM wearable_vitals ;

ALTER TABLE wearable_vitals RENAME COLUMN "timestamp" TO ts;
ALTER TABLE activity_log   RENAME COLUMN "timestamp" TO ts;

CREATE INDEX IF NOT EXISTS ix_vitals_pid_ts ON wearable_vitals(patient_id, ts);
CREATE INDEX IF NOT EXISTS ix_act_pid_ts    ON activity_log(patient_id, ts);
CREATE INDEX IF NOT EXISTS ix_ae_pid_ts     ON ae_reports(patient_id, ae_timestamp);

CREATE OR REPLACE VIEW patient_stream AS
SELECT
  v.patient_id,
  v.ts,
  v.hr,
  v.hrv,
  v.device_on,
  a.activity_type,
  a.steps,
  a.motion_flag
FROM wearable_vitals v
JOIN activity_log a
  ON v.patient_id = a.patient_id
 AND v.ts = a.ts;

 CREATE OR REPLACE VIEW stream_clean AS
SELECT
  patient_id,
  ts,
  device_on,
  activity_type,
  steps,
  motion_flag,

  CASE
    WHEN device_on = 1 AND hr BETWEEN 30 AND 220 THEN hr
    ELSE NULL
  END AS hr_clean,

  CASE
    WHEN device_on = 1 AND hrv BETWEEN 10 AND 250 THEN hrv
    ELSE NULL
  END AS hrv_clean

FROM patient_stream;


CREATE OR REPLACE VIEW signals_step1 AS
SELECT
  *,
  AVG(hrv_clean) OVER (
    PARTITION BY patient_id
    ORDER BY ts
    RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '1 hour' PRECEDING
  ) AS hrv_baseline_7d
FROM stream_clean;


CREATE OR REPLACE VIEW signals_step2 AS
SELECT
  *,
  CASE
    WHEN hrv_clean IS NOT NULL AND hrv_baseline_7d IS NOT NULL
    THEN hrv_clean / NULLIF(hrv_baseline_7d, 0)
    ELSE NULL
  END AS hrv_ratio,

  CASE
    WHEN device_on = 1
     AND activity_type = 'sleep'
     AND motion_flag = 'low'
     AND hrv_clean IS NOT NULL
     AND hrv_baseline_7d IS NOT NULL
     AND (hrv_clean / NULLIF(hrv_baseline_7d, 0)) < 0.85
    THEN 1 ELSE 0
  END AS digital_signal_flag
FROM signals_step1;


CREATE OR REPLACE VIEW signals_final AS
SELECT
  *,
  SUM(digital_signal_flag) OVER (
    PARTITION BY patient_id
    ORDER BY ts
    RANGE BETWEEN INTERVAL '5 hours' PRECEDING AND CURRENT ROW
  ) AS signal_count_last_6h,

  CASE
    WHEN SUM(digital_signal_flag) OVER (
      PARTITION BY patient_id
      ORDER BY ts
      RANGE BETWEEN INTERVAL '5 hours' PRECEDING AND CURRENT ROW
    ) >= 3
    THEN 1 ELSE 0
  END AS persistent_signal
FROM signals_step2;


SELECT
  patient_id,
  MAX(ts) AS last_seen_alert_ts,
  MAX(persistent_signal) AS has_persistent_signal
FROM signals_final
WHERE ts >= (SELECT MAX(ts) FROM signals_final) - INTERVAL '24 hours'
GROUP BY patient_id
HAVING MAX(persistent_signal) = 1
ORDER BY last_seen_alert_ts DESC;


CREATE OR REPLACE VIEW lead_time AS
SELECT
  ae.patient_id,
  ae.ae_timestamp,
  ae.ae_term,
  ae.seriousness,
  MIN(s.ts) FILTER (WHERE s.ts < ae.ae_timestamp) AS first_signal_ts
FROM ae_reports ae
LEFT JOIN signals_final s
  ON s.patient_id = ae.patient_id
 AND s.persistent_signal = 1
GROUP BY
  ae.patient_id, ae.ae_timestamp, ae.ae_term, ae.seriousness;



 CREATE OR REPLACE VIEW lead_time_scored AS
SELECT
  *,
  CASE
    WHEN first_signal_ts IS NOT NULL
    THEN ROUND(EXTRACT(EPOCH FROM (ae_timestamp - first_signal_ts)) / 3600.0, 2)
    ELSE NULL
  END AS lead_time_hours,

  CASE
    WHEN first_signal_ts IS NOT NULL
    THEN ROUND(EXTRACT(EPOCH FROM (ae_timestamp - first_signal_ts)) / 86400.0, 2)
    ELSE NULL
  END AS lead_time_days
FROM lead_time;


-- Do we have any flags?
SELECT COUNT(*) AS digital_flags
FROM signals_final
WHERE digital_signal_flag = 1;

-- Any persistent signals?
SELECT COUNT(*) AS persistent_flags
FROM signals_final
WHERE persistent_signal = 1;

-- Lead time results
SELECT *
FROM lead_time_scored
ORDER BY lead_time_days DESC NULLS LAST;

SELECT
  patient_id, ts,
  activity_type, motion_flag, steps,
  hr_clean, hrv_clean,
  hrv_baseline_7d, hrv_ratio,
  digital_signal_flag, signal_count_last_6h, persistent_signal
FROM signals_final;

SELECT
  patient_id, ae_timestamp, ae_term, seriousness,
  first_signal_ts, lead_time_hours, lead_time_days
FROM lead_time_scored;

SELECT
  COUNT(*) AS ae_total,
  COUNT(first_signal_ts) AS detected_early,
  ROUND(100.0 * COUNT(first_signal_ts)/COUNT(*), 2) AS detection_rate_pct
FROM lead_time_scored;