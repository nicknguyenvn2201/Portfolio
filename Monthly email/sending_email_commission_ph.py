import pandas as pd
import os
from dagster import (
    get_dagster_logger,
    asset,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection
)
import psycopg as pg
from datetime import date
import numpy as np
import pandas as pd
import os
from dateutil.relativedelta import *
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

logger = get_dagster_logger()

path = Path(__file__).parent / "email_list"

def send_mail(recipient,ccRecipient, subject, message,lastmonth_month):  
    username = os.getenv("EMAIL_USERNAME_CONN_CONFIG")
    password = os.getenv("EMAIL_PASSWORD_CONN_CONFIG")

    msg = MIMEMultipart()
    msg['From'] = username
    msg['To'] = ', '.join(recipient) 
    msg["Cc"] = ', '.join(ccRecipient) 
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    file_path = path / f'Fresh_PH_{lastmonth_month}.xlsx'
    with open(file_path, "rb") as file:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{file_path.name}"')
        msg.attach(part)

    file_path2 = path / f'Fresh_PH_sale_order_{lastmonth_month}.xlsx'
    with open(file_path2, "rb") as file:
        part2 = MIMEBase('application', "octet-stream")
        part2.set_payload(file.read())
        encoders.encode_base64(part2)
        part2.add_header('Content-Disposition', f'attachment; filename="{file_path2.name}"')
        msg.attach(part2)

    file_path3 = path / f'Resell_PH_sale_order_{lastmonth_month}.xlsx'
    with open(file_path3, "rb") as file:
        part3 = MIMEBase('application', "octet-stream")
        part3.set_payload(file.read())
        encoders.encode_base64(part3)
        part3.add_header('Content-Disposition', f'attachment; filename="{file_path3.name}"')
        msg.attach(part3)

    try:
        logger.info(f'Sending email to {", ".join(recipient)} with subject "{subject}".')

        all_recipients = recipient + ccRecipient
        mailServer = smtplib.SMTP('smtp-mail.outlook.com', 587)
        mailServer.ehlo()
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(username, password)
        mailServer.sendmail(username, all_recipients, msg.as_string())
        mailServer.close()

        logger.info('Email sent successfully.')

    except Exception  as e:
        logger.error(f"Error sending email: {str(e)}")

def read_emails_commission_ph_from_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def ar_data_extract(month):
    q_PH = f"""
-- AR Fresh Commision
    with full_merge as (
        SELECT 
            flsd.agent_name,
            count(DISTINCT flsd.lead_id) AS leads,
            sum(
                CASE
                    WHEN flsd.lead_status::text = 'approved'::text THEN 1
                    ELSE 0
                END) AS approved,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END) AS validated,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS validated_aov,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS validated_revenue,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END) AS delivered,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS dr,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS delivered_revenue
        FROM fact__lead_sales_delivery flsd
        WHERE flsd.geo::text ^@ 'PH'::text 
        and date_trunc('month', flsd.lead_date) = '{month}'
        and lead_type = 'A'
        group by flsd.agent_name
        )
    select * from full_merge
    

    """
    conn = pg.connect(os.getenv("PG_LIBPQ_CONN_CONFIG"))
    cur = conn.cursor()
    df = pd.read_sql_query(q_PH,conn)
    conn.commit()
    cur.close()
    conn.close()
    return df

def extract_data(month):
    q1 = f"""
    with full_merge as (
        SELECT 
            flsd.agent_name,
            count(DISTINCT flsd.lead_id) AS leads,
            sum(
                CASE
                    WHEN flsd.lead_status::text = 'approved'::text THEN 1
                    ELSE 0
                END) AS approved,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END) AS validated,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS validated_aov,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS validated_revenue,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END) AS delivered,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS dr,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS delivered_revenue
        FROM fact__lead_sales_delivery flsd
        WHERE flsd.geo::text ^@ 'PH'::text 
        and date_trunc('month', so_date) = '{month}'
        and lead_type = 'A'
        group by flsd.agent_name
        )
    select * from full_merge
 
    """
    conn = pg.connect(os.getenv("PG_LIBPQ_CONN_CONFIG"))
    cur = conn.cursor()
    df = pd.read_sql_query(q1,conn)
    conn.commit()
    cur.close()
    conn.close()
    return df 

def extract_data_1(month):
    q2 = f"""
    with full_merge as (
        SELECT 
            flsd.agent_name,
            count(DISTINCT flsd.lead_id) AS leads,
            sum(
                CASE
                    WHEN flsd.lead_status::text = 'approved'::text THEN 1
                    ELSE 0
                END) AS approved,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END) AS validated,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS validated_aov,
            sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS validated_revenue,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END) AS delivered,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN 1
                    ELSE 0
                END)::double precision / NULLIF(sum(
                CASE
                    WHEN flsd.so_status::text = ANY (ARRAY['delay,'::character varying, 'validated'::character varying]::text[]) THEN 1
                    ELSE 0
                END), 0)::double precision AS dr,
            sum(
                CASE
                    WHEN flsd.do_status::text = 'delivered'::text THEN flsd.sales_amount
                    ELSE 0::numeric
                END) AS delivered_revenue
        FROM fact__lead_sales_delivery flsd
        WHERE flsd.geo::text ^@ 'PH'::text 
        AND date_trunc('month', so_date) = '{month}' 
        and lead_type = 'M'
        group by flsd.agent_name
        )
    select * from full_merge
    """
    conn = pg.connect(os.getenv("PG_LIBPQ_CONN_CONFIG"))
    cur = conn.cursor()
    df = pd.read_sql_query(q2,conn)
    conn.commit()
    cur.close()
    conn.close()
    return df

@asset(group_name="send_commission_email_ph")
def sending_email_commission_ph(context: AssetExecutionContext) -> None:
    today = date.today()
    lastmonth_date = (today.replace(day=1) + relativedelta(months=-2)).strftime("%Y-%m-%d")
    lastmonth_month = (today.replace(day=1) + relativedelta(months=-2)).strftime("%b-%y")
    df = ar_data_extract(lastmonth_date)
    df.to_excel(os.path.join(path, f"Fresh_PH_{lastmonth_month}.xlsx"))
    df = extract_data(lastmonth_date)
    df.to_excel(os.path.join(path, f"Fresh_PH_sale_order_{lastmonth_month}.xlsx"))    
    df = extract_data_1(lastmonth_date)
    df.to_excel(os.path.join(path, f"Resell_PH_sale_order_{lastmonth_month}.xlsx"))
    path_to = path / "ToCommissionMonthly_PH.txt"
    recipient = read_emails_commission_ph_from_file(path_to)
    path_cc = path / "CcCommissionMonthly_PH.txt"
    ccRecipient = read_emails_commission_ph_from_file(path_cc)
    subject = 'Philipines Commission Raw Data for '+lastmonth_month
    message = '''Dear all,
    Please find attached to this email the Excel file of Philipines Sales data & AR Data for the month.
    Thanks & Best Regards,'''
    send_mail(recipient,ccRecipient, subject, message,lastmonth_month)


send_email_commission_ph_jobs = define_asset_job(
    name="send_email_commission_ph_jobs",
    selection=AssetSelection.groups("send_commission_email_ph"),
)

send_email_commission_ph_schedule = ScheduleDefinition(
    job=send_email_commission_ph_jobs,
    cron_schedule= "0 6 1 * *", 
    execution_timezone="Asia/Bangkok",
)
