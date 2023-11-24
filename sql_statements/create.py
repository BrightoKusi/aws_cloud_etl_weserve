
dev_schema = 'raw_data'

#----------- Creating the tables for DEV schema(data lake)

call_log = '''CREATE TABLE IF NOT EXISTS raw_data."call_log"
(
	id INT PRIMARY KEY 
	, caller_id VARCHAR
	, agent_id VARCHAR	
	, complaint_topic VARCHAR
 	, assigned_to VARCHAR
  	, status VARCHAR
 	, resolution_duration_in_hours NUMERIC(10, 2)
);'''



call_details = '''CREATE TABLE IF NOT EXISTS raw_data.call_details(
    id INT IDENTITY(1,1) PRIMARY KEY
    ,call_id VARCHAR 
    , call_duration_in_seconds NUMERIC (10, 2)
    , agents_grade_level VARCHAR 
    , call_type VARCHAR
    , call_ended_by_agent VARCHAR 
    , FOREIGN KEY (call_id) REFERENCES raw_data.call_log(id)
);'''

raw_data_tables = ['call_log', 'call_details']





#---------Creating the tables for Staging schema (data warehouse)


dim_call_log = '''CREATE TABLE IF NOT EXISTS staging.dim_call_log(
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , call_log_id VARCHAR
        , caller_id VARCHAR
        , agent_id VARCHAR	
        , complaint_topic VARCHAR
        , assigned_to VARCHAR
        , status VARCHAR
    );'''


dim_call_details = '''CREATE TABLE IF NOT EXISTS staging.dim_call_details(
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , call_details_id VARCHAR
        , call_id VARCHAR
        , agents_grade_level VARCHAR
        , call_type VARCHAR
        , call_ended_by_agent VARCHAR 
);'''


dim_call_duration = '''CREATE TABLE IF NOT EXISTS staging.dim_call_duration( 
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , agent_id VARCHAR
        , agents_grade_level VARCHAR
        , avg_call_duration INT
        , total_call_duration INT
 );'''


dim_call_resolution_status = '''CREATE TABLE IF NOT EXISTS staging.dim_call_resolution_status(
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , agent_id VARCHAR
        , calls_received INT
        , calls_resolved INT
);'''


dim_resolution_time_rank = '''CREATE TABLE IF NOT EXISTS staging.dim_resolution_time_rank(
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , agent_id VARCHAR
        , agents_grade_level VARCHAR
        , resolution_duration_in_hours INT
        , resolution_time_rank INT
);'''


ft_call_transactions = '''CREATE TABLE IF NOT EXISTS staging.ft_call_transactions(
        id BIGINT IDENTITY(1,1) PRIMARY KEY
        , call_log_id INT
        , call_details_id INT
        , call_duration_id INT
        , call_resolution_status_id INT
        , resolution_time_rank_id INT
        , resolution_duration_in_hours INT
        , call_duration_in_seconds INT
        , FOREIGN KEY (call_log_id) REFERENCES staging.dim_call_log (id)
        , FOREIGN KEY (call_details_id) REFERENCES staging.dim_call_details (id)
        , FOREIGN KEY (call_duration_id) REFERENCES staging.dim_call_duration (id)
        , FOREIGN KEY (call_resolution_status_id) REFERENCES staging.dim_call_resolution_status (id)
        , FOREIGN KEY (resolution_time_rank_id) REFERENCES staging.dim_resolution_time_rank (id)
);'''


transformed_tables = ['dim_call_log', 'dim_call_details', 'dim_call_duration', 'dim_call_resolution_status', 'dim_resolution_time_rank', 'ft_call_transactions']


