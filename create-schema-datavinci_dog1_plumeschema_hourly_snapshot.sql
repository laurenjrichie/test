-- Databricks notebook source



------
-- red database is the original paths we were using till today
CREATE DATABASE IF NOT EXISTS datavinci_dog1_plumeschema_hourly_snapshot;
USE datavinci_dog1_plumeschema_hourly_snapshot;

CREATE TABLE IF NOT EXISTS `table_device_master_dimension`(
  `location_id` string,
  `node_pk` string,
  `idtype` string,
  `id` string,
  `version` string,
  `is_device` boolean,
  `mcs_capability` int,
  `nss_capability` int,
  `bw_capability` int,
  `phy_rate_capability` int,
  `is_static` boolean,
  `band_steering_enabled` boolean,
  `client_steering_enabled` boolean,
  `bs_disable_reasons` string,
  `cs_disable_reasons` string,
  `medium` string,
  `connection_state` string,
  `connection_state_change_at` bigint,
  `id_firstseen` timestamp,
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
USING AVRO
COMMENT 'Device Master dimension Hourly Snapshot'
PARTITIONED BY (
  `year`,
  `month`,
  `day`,
  `hour`)
LOCATION
  's3://plume-development-us-west-2-cloud/type=datawarehouse-avrodata-hourly-snapshot/environment=dogfood/deployment=dog1/table=device_master_dimension/';
MSCK REPAIR TABLE table_device_master_dimension;

CREATE TABLE IF NOT EXISTS `table_gateway_master_dimension`(
  `location_id` string,
  `node_pk` string,
  `idtype` string,
  `id` string,
  `version` string,
  `is_device` boolean,
  `mcs_capability` int,
  `nss_capability` int,
  `bw_capability` int,
  `phy_rate_capability` int,
  `firmware_version` string,
  `id_firstseen` timestamp,
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
USING AVRO
COMMENT 'Gateway Master dimension Hourly Snapshot'
PARTITIONED BY (
  `year`,
  `month`,
  `day`,
  `hour`)
LOCATION
  's3://plume-development-us-west-2-cloud/type=datawarehouse-avrodata-hourly-snapshot/environment=dogfood/deployment=dog1/table=gateway_master_dimension/';
MSCK REPAIR TABLE table_gateway_master_dimension;

CREATE TABLE IF NOT EXISTS `table_group_master_dimension`(
  `group_id` string,
  `customer_id` string,
  `name` string,
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
USING AVRO
COMMENT 'Group Master dimension Hourly Snapshot'
PARTITIONED BY (
  `year`,
  `month`,
  `day`,
  `hour`)
LOCATION
  's3://plume-development-us-west-2-cloud/type=datawarehouse-avrodata-hourly-snapshot/environment=dogfood/deployment=dog1/table=group_master_dimension/';
MSCK REPAIR TABLE table_group_master_dimension;

CREATE TABLE  IF NOT EXISTS `table_location_master_dimension`(
  `location_id` string,
  `customer_id` string,
  `num_nodes` int,
  `monitor_mode` boolean,
  `control_mode` string,
  `mode_updated_at` bigint,
  `account_id` string,
  `claimed_nodes` int,
  `state` string,
  `isp` string,
  `zip` string,
  `province` string,
  `city` string,
  `country` string,
  `latitude` double,
  `longitude` double,
  `timezone` string,
  `wifiisocode` string,
  `tzwithabbr` string,
  `tzoffset` int,
  `partner_id` string,
  `service_level` string,
  `service_id` string,
  `network_mode_realized` string,
  `is_motion_enabled` boolean,
  `created_at` timestamp,
  `onboarded_at` timestamp,
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
USING AVRO
COMMENT 'Location Master dimension Hourly Snapshot'
PARTITIONED BY (
  `year`,
  `month`,
  `day`,
  `hour`)
LOCATION
  's3://plume-development-us-west-2-cloud/type=datawarehouse-avrodata-hourly-snapshot/environment=dogfood/deployment=dog1/table=location_master_dimension/';
MSCK REPAIR TABLE table_location_master_dimension;

CREATE TABLE IF NOT EXISTS `table_node_master_dimension`(
  `location_id` string,
  `node_pk` string,
  `idtype` string,
  `id` string,
  `version` string,
  `is_device` boolean,
  `mcs_capability` int,
  `nss_capability` int,
  `bw_capability` int,
  `phy_rate_capability` int,
  `medium` string,
  `connection_state` string,
  `connection_state_change_at` bigint,
  `id_firstseen` timestamp,
  `recordtype` string,
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
USING AVRO
COMMENT 'Node Master dimension Hourly Snapshot'
PARTITIONED BY (
  `recordtype`,
  `year`,
  `month`,
  `day`,
  `hour`)
LOCATION
  's3://plume-development-us-west-2-cloud/type=datawarehouse-avrodata-hourly-snapshot/environment=dogfood/deployment=dog1/table=node_master_dimension/';
MSCK REPAIR TABLE table_node_master_dimension;

------
-- generate and point original athena names at blue path
-- Tonic leaves out table= prefix in blue path

