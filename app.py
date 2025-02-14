from flask import Flask, jsonify
import os
import snowflake.connector
print(snowflake.connector.__version__)
from snowflake.connector.errors import ProgrammingError

# --- Environment & Directory Setup ---
os.environ['HOME'] = '/tmp'  # Replace with your accessible path
os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'] = os.path.expanduser('~/.snowflake')

if not os.path.exists(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']):
    os.makedirs(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'], exist_ok=True)

# --- Flask App Initialization ---
app = Flask(__name__)

def execute_snowflake_query():
    # Snowflake connection parameters
    conn_params = {
        'user': 'Anoop',
        'password': 'Parjanya@10',
        'account': 'LOA94655',
        'role': 'AMBIT_DL_USER',  
        'database': 'AMBIT_DB',
        'warehouse': 'AMBIT_DL_WH'
    }
    
    def table_exists(cursor, schema_name, table_name):
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s
            """, (schema_name, table_name))
            
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            print(f"Error checking table existence: {str(e)}")
            return False

    try:
        # Establish connection
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create schema if not exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS AMBIT_DB.CORE_APP")
        cursor.execute("USE SCHEMA AMBIT_DB.CORE_APP")

        append_criteria_1 = """
        INSERT INTO CORE_APP.CRITERIA_1 
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria1_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Combine AS (
            SELECT * FROM Criteria1_Dx
            UNION ALL
            SELECT * FROM Criteria1_Px
            UNION ALL
            SELECT * FROM Criteria1_Rx
        ),
        Criteria1_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria1_Combine
            GROUP BY forian_patient_id
        ),
        Criteria1_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria1_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria1_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria1_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria1_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria1_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria1_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario 
        FROM Criteria1_Combine fx
        INNER JOIN Criteria1_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria1_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        append_criteria_2 = """
        INSERT INTO CORE_APP.CRITERIA_2 
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria2_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Combine AS (
            SELECT * FROM Criteria2_Dx
            UNION ALL
            SELECT * FROM Criteria2_Px
            UNION ALL
            SELECT * FROM Criteria2_Rx
        ),
        Criteria2_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria2_Combine
            GROUP BY forian_patient_id
        ),
        Criteria2_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria2_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria2_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria2_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria2_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria2_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria2_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario
        FROM Criteria2_Combine fx
        INNER JOIN Criteria2_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria2_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        append_criteria_3 = """
        INSERT INTO CORE_APP.CRITERIA_3 
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria3_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Combine AS (
            SELECT * FROM Criteria3_Dx
            UNION ALL
            SELECT * FROM Criteria3_Px
            UNION ALL
            SELECT * FROM Criteria3_Rx
        ),
        Criteria3_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria3_Combine
            GROUP BY forian_patient_id
        ),
        Criteria3_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria3_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria3_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria3_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria3_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria3_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria3_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario
        FROM Criteria3_Combine fx
        INNER JOIN Criteria3_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria3_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        append_criteria_12 = """
        INSERT INTO CORE_APP.CRITERIA_12_PATIENT 
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
		Criteria_12_patient AS (
            SELECT 
                cr1.forian_patient_id, 
                COALESCE(cr1.date_of_service, cr2.date_of_service) AS date_of_service,
                COALESCE(cr1.birth_year, cr2.birth_year) AS birth_year,
                COALESCE(cr1.gender, cr2.gender) AS gender,
                COALESCE(cr1.subscriber_address_state, cr2.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr1.Patient_Age, cr2.Patient_Age) AS Patient_Age,
                COALESCE(cr1.Ambit_Grouping, cr2.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr1.Source, cr2.Source) AS Source,
                COALESCE(cr1.age_group, cr2.age_group) AS age_group,
                COALESCE(cr1.Scenario, cr2.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_1 cr1
            INNER JOIN CORE_APP.CRITERIA_2 cr2 
                ON cr1.forian_patient_id = cr2.forian_patient_id
				AND cr1.Scenario = cr2.Scenario
            WHERE COALESCE(cr1.Scenario, cr2.Scenario) IN (SELECT "Scenario" FROM Latest_Record_Config)
			AND ((SELECT DISTINCT("Criteria12_Condition") FROM Latest_Record_Config) = 'AND' 
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            ))

            UNION

            SELECT 
                COALESCE(cr1.forian_patient_id, cr2.forian_patient_id) AS forian_patient_id,
                COALESCE(cr1.date_of_service, cr2.date_of_service) AS date_of_service,
                COALESCE(cr1.birth_year, cr2.birth_year) AS birth_year,
                COALESCE(cr1.gender, cr2.gender) AS gender,
                COALESCE(cr1.subscriber_address_state, cr2.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr1.Patient_Age, cr2.Patient_Age) AS Patient_Age,
                COALESCE(cr1.Ambit_Grouping, cr2.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr1.Source, cr2.Source) AS Source,
                COALESCE(cr1.age_group, cr2.age_group) AS age_group,
                COALESCE(cr1.Scenario, cr2.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_1 cr1
            FULL OUTER JOIN CORE_APP.CRITERIA_2 cr2 
                ON cr1.forian_patient_id = cr2.forian_patient_id
				AND cr1.Scenario = cr2.Scenario
            WHERE COALESCE(cr1.Scenario, cr2.Scenario) IN (SELECT "Scenario" FROM Latest_Record_Config)
			AND ((SELECT DISTINCT("Criteria12_Condition") FROM Latest_Record_Config) = 'OR'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            ))
        )
        SELECT DISTINCT * FROM Criteria_12_patient
        """

        append_all_criteria = """
        INSERT INTO CORE_APP.ALL_CRITERIA_PATIENT 
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
		All_Criteria_patient AS (
            SELECT 
                cr12.forian_patient_id, 
                COALESCE(cr12.birth_year, cr3.birth_year) AS birth_year,
                COALESCE(cr12.gender, cr3.gender) AS gender,
                COALESCE(cr12.subscriber_address_state, cr3.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr12.Patient_Age, cr3.Patient_Age) AS Patient_Age,
                COALESCE(cr12.Ambit_Grouping, cr3.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr12.Source, cr3.Source) AS Source,
                COALESCE(cr12.age_group, cr3.age_group) AS age_group,
                COALESCE(cr12.Scenario, cr3.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_12_PATIENT cr12
            INNER JOIN CORE_APP.CRITERIA_3 cr3 
                ON cr12.forian_patient_id = cr3.forian_patient_id
				AND cr12.Scenario = cr3.Scenario
            WHERE COALESCE(cr12.Scenario, cr3.Scenario) IN (SELECT "Scenario" FROM Latest_Record_Config)
            AND ((SELECT DISTINCT("Criteria123_Condition") FROM Latest_Record_Config) = 'AND'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            ))

            UNION

            SELECT 
                COALESCE(cr12.forian_patient_id, cr3.forian_patient_id) AS forian_patient_id,
                COALESCE(cr12.birth_year, cr3.birth_year) AS birth_year,
                COALESCE(cr12.gender, cr3.gender) AS gender,
                COALESCE(cr12.subscriber_address_state, cr3.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr12.Patient_Age, cr3.Patient_Age) AS Patient_Age,
                COALESCE(cr12.Ambit_Grouping, cr3.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr12.Source, cr3.Source) AS Source,
                COALESCE(cr12.age_group, cr3.age_group) AS age_group,
                COALESCE(cr12.Scenario, cr3.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_12_PATIENT cr12
            FULL OUTER JOIN CORE_APP.CRITERIA_3 cr3 
                ON cr12.forian_patient_id = cr3.forian_patient_id
				AND cr12.Scenario = cr3.Scenario
            WHERE COALESCE(cr12.Scenario, cr3.Scenario) IN (SELECT "Scenario" FROM Latest_Record_Config)
			AND ((SELECT DISTINCT("Criteria123_Condition") FROM Latest_Record_Config) = 'OR'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            ))
        )
        SELECT DISTINCT * FROM All_Criteria_patient
        """

        create_criteria_1 = """
        CREATE OR REPLACE TABLE CORE_APP.CRITERIA_1 AS
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria1_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria1_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_1' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria1_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria1_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria1_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria1_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria1_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria1_Combine AS (
            SELECT * FROM Criteria1_Dx
            UNION ALL
            SELECT * FROM Criteria1_Px
            UNION ALL
            SELECT * FROM Criteria1_Rx
        ),
        Criteria1_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria1_Combine
            GROUP BY forian_patient_id
        ),
        Criteria1_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria1_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria1_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria1_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria1_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria1_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria1_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario 
        FROM Criteria1_Combine fx
        INNER JOIN Criteria1_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria1_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        create_criteria_2 = """
        CREATE OR REPLACE TABLE CORE_APP.CRITERIA_2 AS
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria2_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria2_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_2' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria2_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria2_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria2_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria2_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria2_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria2_Combine AS (
            SELECT * FROM Criteria2_Dx
            UNION ALL
            SELECT * FROM Criteria2_Px
            UNION ALL
            SELECT * FROM Criteria2_Rx
        ),
        Criteria2_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria2_Combine
            GROUP BY forian_patient_id
        ),
        Criteria2_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria2_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria2_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria2_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria2_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria2_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria2_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario
        FROM Criteria2_Combine fx
        INNER JOIN Criteria2_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria2_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        create_criteria_3 = """
        CREATE OR REPLACE TABLE CORE_APP.CRITERIA_3 AS
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
        Criteria3_Dx AS (
            SELECT DISTINCT 
                dx.forian_patient_id, 
                dx.date_of_service, 
                dx.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Dx' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_DIAGNOSIS dx 
            INNER JOIN Latest_Record_Config cnfg 
                ON dx.d_diagnosis_code = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON dx.forian_patient_id = pt.forian_patient_id
            WHERE dx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Px AS (
            SELECT DISTINCT 
                px.forian_patient_id, 
                px.date_of_service, 
                px.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Px' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario
            FROM AMBIT_DB.FORIAN.OPEN_MEDICAL_HOSPITAL_CLAIM_PROCEDURE px 
            INNER JOIN Latest_Record_Config cnfg 
                ON px.D_PROCEDURE_CODE = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON px.forian_patient_id = pt.forian_patient_id
            WHERE px.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Rx AS (
            SELECT DISTINCT 
                rx.forian_patient_id, 
                rx.date_of_service, 
                rx.claim_number, 
                cnfg."Criteria3_grp" AS grp,
                pt.GENDER,
                pt.subscriber_address_state,
                pt.birth_year,
                YEAR(CURRENT_DATE) - pt.birth_year as Patient_Age,
                'Rx' as Source,
                'Criteria_3' as Criteria,
                CASE 
                    WHEN pt.birth_year IS NULL THEN 'NA'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year <= 18 THEN '0-18'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 19 AND 30 THEN '19-30'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 31 AND 50 THEN '31-50'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year BETWEEN 51 AND 60 THEN '51-60'
                    WHEN YEAR(CURRENT_DATE) - pt.birth_year > 60 THEN '60+'
                    ELSE 'NA'
                END AS age_group,
                cnfg."Scenario" as Scenario 
            FROM AMBIT_DB.FORIAN.OPEN_PHARMACY_TRANSACTION rx 
            INNER JOIN Latest_Record_Config cnfg 
                ON rx.ndc = cnfg."Criteria3_Code"
            LEFT JOIN AMBIT_DB.CIAI_ANALYTICS.PATIENT_REFERENCE_UNIQUE pt
                ON rx.forian_patient_id = pt.forian_patient_id
            WHERE rx.date_of_service BETWEEN 
                (SELECT DISTINCT("Criteria3_Startdate") FROM Latest_Record_Config) 
                AND 
                (SELECT DISTINCT("Criteria3_Enddate") FROM Latest_Record_Config)
            AND (
                ((SELECT DISTINCT("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '<=' 
                    AND pt.birth_year <= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
                OR
                ((SELECT MAX("Criteria3_Birthyear_Condition") FROM Latest_Record_Config) = '>=' 
                    AND pt.birth_year >= (SELECT DISTINCT("Criteria3_Birthyear") FROM Latest_Record_Config))
            )
        ),
        Criteria3_Combine AS (
            SELECT * FROM Criteria3_Dx
            UNION ALL
            SELECT * FROM Criteria3_Px
            UNION ALL
            SELECT * FROM Criteria3_Rx
        ),
        Criteria3_Grp_Count AS (
            SELECT forian_patient_id, COUNT(DISTINCT grp) AS grp_count
            FROM Criteria3_Combine
            GROUP BY forian_patient_id
        ),
        Criteria3_Patients AS (
            SELECT forian_patient_id 
            FROM Criteria3_Grp_Count 
            WHERE grp_count >= 
                CASE 
                    WHEN (SELECT DISTINCT("Criteria3_Condition") FROM Latest_Record_Config) = 'AND' 
                    THEN (SELECT COUNT(DISTINCT "Criteria3_grp") FROM Latest_Record_Config)
                    ELSE 1
                END
        ),
        Criteria3_clm_cnt AS (
            SELECT forian_patient_id 
            FROM Criteria3_Combine 
            GROUP BY forian_patient_id 
            HAVING COUNT(DISTINCT claim_number) >= (SELECT DISTINCT("Criteria3_clm_count") FROM Latest_Record_Config)
        )
        SELECT DISTINCT 
            fx.forian_patient_id, 
            fx.date_of_service,
            fx.GENDER,
            fx.subscriber_address_state,
            fx.birth_year,
            fx.Source,
            fx.Patient_Age,
            fx.grp as Ambit_Grouping,
            fx.Criteria,
            fx.age_group,
            fx.Scenario
        FROM Criteria3_Combine fx
        INNER JOIN Criteria3_Patients a
            ON fx.forian_patient_id = a.forian_patient_id
        INNER JOIN Criteria3_clm_cnt b
            ON fx.forian_patient_id = b.forian_patient_id
        """

        # Create Criteria_12_patient table
        create_criteria_12 = """
        CREATE OR REPLACE TABLE CORE_APP.CRITERIA_12_PATIENT AS
        WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
		Criteria_12_patient AS (
            SELECT 
                cr1.forian_patient_id, 
                COALESCE(cr1.date_of_service, cr2.date_of_service) AS date_of_service,
                COALESCE(cr1.birth_year, cr2.birth_year) AS birth_year,
                COALESCE(cr1.gender, cr2.gender) AS gender,
                COALESCE(cr1.subscriber_address_state, cr2.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr1.Patient_Age, cr2.Patient_Age) AS Patient_Age,
                COALESCE(cr1.Ambit_Grouping, cr2.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr1.Source, cr2.Source) AS Source,
                COALESCE(cr1.age_group, cr2.age_group) AS age_group,
                COALESCE(cr1.Scenario, cr2.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_1 cr1
            INNER JOIN CORE_APP.CRITERIA_2 cr2 
                ON cr1.forian_patient_id = cr2.forian_patient_id
            WHERE (SELECT DISTINCT("Criteria12_Condition") FROM Latest_Record_Config) = 'AND' 
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            )

            UNION

            SELECT 
                COALESCE(cr1.forian_patient_id, cr2.forian_patient_id) AS forian_patient_id,
                COALESCE(cr1.date_of_service, cr2.date_of_service) AS date_of_service,
                COALESCE(cr1.birth_year, cr2.birth_year) AS birth_year,
                COALESCE(cr1.gender, cr2.gender) AS gender,
                COALESCE(cr1.subscriber_address_state, cr2.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr1.Patient_Age, cr2.Patient_Age) AS Patient_Age,
                COALESCE(cr1.Ambit_Grouping, cr2.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr1.Source, cr2.Source) AS Source,
                COALESCE(cr1.age_group, cr2.age_group) AS age_group,
                COALESCE(cr1.Scenario, cr2.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_1 cr1
            FULL OUTER JOIN CORE_APP.CRITERIA_2 cr2 
                ON cr1.forian_patient_id = cr2.forian_patient_id
            WHERE (SELECT DISTINCT("Criteria12_Condition") FROM Latest_Record_Config) = 'OR'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria12_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr1.date_of_service, '1900-01-01'), COALESCE(cr2.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria12_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            )
        )
        SELECT DISTINCT * FROM Criteria_12_patient
        """

        # Create ALL_CRITERIA_PATIENT table
        create_all_criteria = """
        CREATE OR REPLACE TABLE CORE_APP.ALL_CRITERIA_PATIENT AS
         WITH Ranked_scenario AS (
            SELECT "Scenario",
                "DateTime",
                DENSE_RANK() OVER (ORDER BY "DateTime" DESC) AS rnk
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
        ),
        Latest_Record_Config AS (
            SELECT * 
            FROM AMBIT_DB.CORE_APP."Patient_Qualification_Final_Table"
            WHERE "Scenario" = (
                SELECT "Scenario"
                FROM Ranked_scenario
                WHERE rnk = 1
                LIMIT 1
            )
        ),
		All_Criteria_patient AS (
            SELECT 
                cr12.forian_patient_id, 
                COALESCE(cr12.birth_year, cr3.birth_year) AS birth_year,
                COALESCE(cr12.gender, cr3.gender) AS gender,
                COALESCE(cr12.subscriber_address_state, cr3.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr12.Patient_Age, cr3.Patient_Age) AS Patient_Age,
                COALESCE(cr12.Ambit_Grouping, cr3.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr12.Source, cr3.Source) AS Source,
                COALESCE(cr12.age_group, cr3.age_group) AS age_group,
                COALESCE(cr12.Scenario, cr3.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_12_PATIENT cr12
            INNER JOIN CORE_APP.CRITERIA_3 cr3 
                ON cr12.forian_patient_id = cr3.forian_patient_id
            WHERE (SELECT DISTINCT("Criteria123_Condition") FROM Latest_Record_Config) = 'AND'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            )

            UNION

            SELECT 
                COALESCE(cr12.forian_patient_id, cr3.forian_patient_id) AS forian_patient_id,
                COALESCE(cr12.birth_year, cr3.birth_year) AS birth_year,
                COALESCE(cr12.gender, cr3.gender) AS gender,
                COALESCE(cr12.subscriber_address_state, cr3.subscriber_address_state) AS subscriber_address_state,
                COALESCE(cr12.Patient_Age, cr3.Patient_Age) AS Patient_Age,
                COALESCE(cr12.Ambit_Grouping, cr3.Ambit_Grouping) AS Ambit_Grouping,
                COALESCE(cr12.Source, cr3.Source) AS Source,
                COALESCE(cr12.age_group, cr3.age_group) AS age_group,
                COALESCE(cr12.Scenario, cr3.Scenario) AS Scenario
            FROM CORE_APP.CRITERIA_12_PATIENT cr12
            FULL OUTER JOIN CORE_APP.CRITERIA_3 cr3 
                ON cr12.forian_patient_id = cr3.forian_patient_id
            WHERE (SELECT DISTINCT("Criteria123_Condition") FROM Latest_Record_Config) = 'OR'
            AND (
                CASE 
                    WHEN COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') <> '' 
                    THEN 
                        (
                            (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    > COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<'  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    < COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '>='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    >= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                            
                            OR (COALESCE((SELECT MAX("Criteria123_list_condition") FROM Latest_Record_Config), '') = '<='  
                                AND DATEDIFF(day, COALESCE(cr12.date_of_service, '1900-01-01'), COALESCE(cr3.date_of_service, '1900-01-01')) 
                                    <= COALESCE((SELECT MAX("Criteria123_no_of_days") FROM Latest_Record_Config), 0))
                        )
                    ELSE TRUE
                END
            )
        )
        SELECT DISTINCT * FROM All_Criteria_patient
        """
        try:

            tables = {
                'CRITERIA_1': {'create': create_criteria_1, 'append': append_criteria_1},
                'CRITERIA_2': {'create': create_criteria_2, 'append': append_criteria_2},
                'CRITERIA_3': {'create': create_criteria_3, 'append': append_criteria_3},
                'CRITERIA_12_PATIENT': {'create': create_criteria_12, 'append': append_criteria_12},
                'ALL_CRITERIA_PATIENT': {'create': create_all_criteria, 'append': append_all_criteria}
            }
                
            for table, queries in tables.items():
                if table_exists(cursor, 'CORE_APP', table):
                    print(f"Appending data to {table} table...")
                    cursor.execute(queries['append'])
                else:
                    print(f"Creating {table} table...")
                    cursor.execute(queries['create'])

            print("All tables processed successfully!")

            # Verify the table records count
            for table in tables.keys():
                cursor.execute(f"SELECT COUNT(*) FROM CORE_APP.{table}")
                count = cursor.fetchone()[0]
                print(f"Total records in {table}: {count}")
            print("All tables processed successfully!")

            # Verify the table records count
            for table in tables.keys():
                cursor.execute(f"SELECT COUNT(*) FROM CORE_APP.{table}")
                count = cursor.fetchone()[0]
                print(f"Total records in {table}: {count}")       

        except ProgrammingError as e:
            print(f"Error executing query: {str(e)}")
            raise
            
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        raise
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("Connection closed.")
# --- Flask Route to Run the Query ---

@app.route('/test',methods=['GET'])

def testing():
    return 'app is working'

@app.route('/run', methods=['GET'])
def run_snowflake():
    try:
        log_messages = execute_snowflake_query()
        return jsonify({"status": "success", "logs": log_messages})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# --- Run the Flask App ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

