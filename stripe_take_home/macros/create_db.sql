{% macro create_analytics_db() %}
  {% set sql %}
    CREATE DATABASE IF NOT EXISTS ANALYTICS;
    CREATE SCHEMA IF NOT EXISTS ANALYTICS.Stripe;
  {% endset %}
  {% do run_query(sql) %}
  {{ log("Created database ANALYTICS and schema Stripe", info=True) }}
{% endmacro %}
