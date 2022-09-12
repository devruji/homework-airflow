from airflow import DAG
from airflow.models import XCom
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

import os
import logging
import pandas as pd

BASE_DIR = os.path.join(os.getcwd(), 'dags')

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id='sunday_pipeline',
    start_date=datetime(2022, 9, 12),
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    description='policy information for corporate health insurance',
    tags=['Hello Sunday']
) as dag:
    start = DummyOperator(
        task_id='start'
    )

    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='postgres_con',
        sql='''
            DROP TABLE if EXISTS 
                company_list, 
                plan_list, 
                customer_list, 
                employee_addition;
          ''',
    )
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_con',
        sql='''
            -- Company List
            CREATE TABLE IF NOT EXISTS company_list (
                name VARCHAR (50) PRIMARY KEY NOT NULL, 
                district VARCHAR (50) NOT NULL, 
                province VARCHAR (50) NOT NULL, 
                phone_no VARCHAR (50) NOT NULL, 
                effective_date DATE NOT NULL, 
                expiry_date DATE NOT NULL
            );
            -- Plan List
            CREATE TABLE IF NOT EXISTS plan_list (
                plan VARCHAR (1) UNIQUE PRIMARY KEY NOT NULL, 
                description VARCHAR (500) NOT NULL
            );
            -- Customer List
            CREATE TABLE IF NOT EXISTS customer_list (
                name VARCHAR (50) NOT NULL, 
                first_name VARCHAR (50) NOT NULL, 
                last_name VARCHAR (50) NOT NULL, 
                ID VARCHAR (50) PRIMARY KEY NOT NULL, 
                plan VARCHAR (1) NOT NULL, 
                gender VARCHAR (1) NOT NULL, 
                district VARCHAR (50) NOT NULL, 
                province VARCHAR (50) NOT NULL, 
                preferred_hospital VARCHAR (50) NOT NULL,
                FOREIGN KEY (name) REFERENCES company_list (name), 
                FOREIGN KEY (plan) REFERENCES plan_list (plan)
            );
            -- Employee Addition
            CREATE TABLE IF NOT EXISTS employee_addition (
                name VARCHAR (50) NOT NULL, 
                first_name VARCHAR (50) NOT NULL, 
                last_name VARCHAR (50) NOT NULL, 
                ID VARCHAR (50) PRIMARY KEY NOT NULL, 
                plan VARCHAR (1) NOT NULL, 
                gender VARCHAR (1) NOT NULL, 
                district VARCHAR (50) NOT NULL, 
                province VARCHAR (50) NOT NULL, 
                preferred_hospital VARCHAR (50) NOT NULL, 
                effective_date DATE NOT NULL,
                FOREIGN KEY (name) REFERENCES company_list (name), 
                FOREIGN KEY (plan) REFERENCES plan_list (plan)
            );
          ''',
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_con',
        sql='''
            -- Company List
            INSERT INTO company_list(name, district, province, phone_no, effective_date, expiry_date) 
            VALUES 
            ('AAA Co', 'Bangrak', 'Bangkok', '02-123-4567', '1-Jan-21', '1-Jan-22');
            
            -- Plan List
            INSERT INTO plan_list(plan, description) 
            VALUES 
            ('A', 'aaaaa'),
            ('B', 'bbbbb'),
            ('C', 'ccccc');
            
            -- Customer List
            INSERT INTO customer_list(name, first_name, last_name, id, plan, gender, district, province, preferred_hospital) 
            VALUES 
            ('AAA Co', 'Mary', 'One', '01-1111', 'A', 'M', 'Wattana', 'Bangkok', 'Rajvithi'),
            ('AAA Co', 'Sue', 'Two', '1-1-200', 'A', 'F', 'Jatujak', 'Bkk', 'Siriaj'),
            ('AAA Co', 'Luke', 'Three', '1-1-300', 'B', 'F', 'Mae Sot', 'Tak', 'Rajvithi'),
            ('AAA Co', 'Charlie', 'Four', '1-1-400', 'B', 'M', 'Pak Kret', 'Nonthaburi', 'Rama'),
            ('AAA Co', 'Parker', 'Five', '11115', 'C', 'M', 'Bangrak', 'Bangkok', 'Siriraj');
            
            -- Employee Addition
            INSERT INTO employee_addition(name, first_name, last_name, id, plan, gender, district, province, preferred_hospital, effective_date) 
            VALUES 
            ('AAA Co', 'John', 'Six', '01-11-6', 'A', 'F', 'Prawet', 'BKK', 'Siriraj', '31-Jan-21'),
            ('AAA Co', 'Susan', 'Seven', '0111-7', 'C', 'F', 'Bangna', 'Bangkok', 'Rajvithi', '31-Jan-21'),
            ('AAA Co', 'Luke', 'Eight', '1-1-1111', 'B', 'M', 'Bangrak', 'Bangkok', 'Rama', '31-Jan-21');
        '''
    )
    
    with TaskGroup(group_id='gen_report') as tg:
        hook = PostgresHook(postgres_conn_id='postgres_con').get_sqlalchemy_engine()
        if not os.path.exists(os.path.join(BASE_DIR, 'output')):
            os.makedirs(os.path.join(BASE_DIR, 'output'))

        @task()
        def report_a():
            sql = '''
                select
                    name,
                    plan,
                    COUNT(*)
                from (
                    select 
                        name,
                        plan
                    from postgres.public.customer_list
                    union all
                    select 
                        name,
                        plan
                    from postgres.public.employee_addition
                ) tbl1
                group by name, plan
                order by plan;
            '''
            df = pd.read_sql(sql=sql, con=hook)
            df.to_csv(os.path.join(BASE_DIR, 'output', 'report_a.csv'), index=False)
        
        @task()
        def report_b():
            sql = '''
                select
                    name,
                    plan,
                    COUNT(*)
                from (
                    select 
                        name,
                        plan
                    from postgres.public.customer_list
                    union all
                    select 
                        name,
                        plan
                    from postgres.public.employee_addition
                ) tbl1
                group by name, plan
                order by plan;
            '''
            df = pd.read_sql(sql='SELECT * FROM customer_list', con=hook)
            df.to_csv(os.path.join(BASE_DIR, 'output', 'report_b.csv'), index=False)

        @task()
        def report_c():
            sql = '''
                select 
                    name,
                    effective_date,
                    expiry_date,
                    date_part('year', expiry_date) - date_part('year', effective_date) as customer_been_with_us_year
                from company_list
            '''
            df = pd.read_sql(sql='SELECT * FROM customer_list', con=hook)
            df.to_csv(os.path.join(BASE_DIR, 'output', 'report_c.csv'), index=False)
            
        [report_a(), report_b(), report_c()]
    
    @task
    @provide_session
    def cleanup_xcom(session=None, **context):
        dag = context['dag']
        dag_id = dag._dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    end = DummyOperator(
        task_id='end'
    )
    
    start >> drop_table >> create_table >> load_data >> tg >> cleanup_xcom() >> end
