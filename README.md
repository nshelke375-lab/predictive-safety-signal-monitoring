🏥 Predictive Safety Signal Monitoring System

An end-to-end healthcare data analytics project that detects early physiological safety signals from wearable sensor data using SQL time-series analysis and visualizes patient risk in a Power BI monitoring dashboard.

The system identifies abnormal HRV patterns and predicts potential adverse events before clinical symptoms are reported, enabling proactive patient monitoring.

🚀 Project Overview

Wearable health devices generate continuous physiological data such as Heart Rate (HR) and Heart Rate Variability (HRV). However, identifying clinically meaningful patterns from this noisy time-series data is challenging.

This project builds a clinical safety signal detection pipeline that transforms raw wearable data into actionable alerts using SQL feature engineering and rolling baseline analysis.

The final output is an interactive Power BI dashboard that allows clinicians or safety analysts to monitor patient risk in real time.

💡 Key Results

Early Safety Detection

The system successfully identified 56% of adverse events before they were clinically reported.

Predictive Lead Time

Detected abnormal physiological signals 2.03 hours before the reported event on average, providing a valuable intervention window.

Noise Reduction

Implemented persistent signal logic requiring 3 alerts within a 6-hour window, reducing false alarms caused by wearable sensor noise.

🧠 Problem Statement

Healthcare monitoring systems often struggle with:

noisy wearable sensor data

high false-alert rates

difficulty identifying early warning signs

This project demonstrates how time-series analytics and SQL feature engineering can extract meaningful safety signals from wearable data.

🛠️ Tech Stack
Data Processing
SQL (PostgreSQL)
Analytics Techniques
Time-Series Analysis
Rolling Baseline Modeling
Window Functions
Feature Engineering
Signal Detection Logic
Visualization
Power BI
Data Source
Synthetic Wearable Patient Data
Heart Rate (HR)
Heart Rate Variability (HRV)
Activity Context (Sleep / Run / Walk)
Adverse Event Reports
🏗️ Data Architecture
This project follows a Medallion Architecture to ensure reliable data processing.
🥉 Bronze Layer – Raw Data
Raw wearable and clinical event datasets were ingested.

wearable_vitals
activity_log
ae_reports

These tables contain timestamped physiological signals and activity context.

🥈 Silver Layer – Cleaned & Feature Engineered

Data cleaning and feature engineering were performed using SQL.

Key transformations include:

filtering records where wearable devices were active

calculating rolling physiological baselines

detecting abnormal HRV patterns

identifying digital safety signals

Rolling HRV Baseline
AVG(hrv_clean)
OVER (PARTITION BY patient_id ORDER BY ts 
ROWS BETWEEN 168 PRECEDING AND CURRENT ROW)

This computes a dynamic 7-day HRV baseline for each patient.

🥇 Gold Layer – Predictive Safety Signals

The final analytical layer produces actionable clinical insights.

Signal detection rule:

HRV < baseline * 0.85
AND activity_type = 'sleep'

Persistent alert logic:

3 signals detected within a 6-hour window

This reduces noise and identifies meaningful physiological anomalies.

📊 Dashboard Overview

The Power BI dashboard enables interactive monitoring of patient safety signals.

Patient Safety Signal Heatmap

Displays daily safety alerts across patients to quickly identify abnormal activity patterns.

Safety Signal Timeline

Visualizes when physiological anomalies occur across different activity contexts.

HRV Biomarker Trend

Shows HRV deviations compared to the rolling baseline.

Patient Risk Ranking

Ranks patients by alert frequency to prioritize clinical investigation.

📈 Key Insights

HRV drops during sleep strongly correlate with abnormal physiological signals

Persistent alerts significantly reduce false positives

Digital biomarkers can provide early detection before adverse events
