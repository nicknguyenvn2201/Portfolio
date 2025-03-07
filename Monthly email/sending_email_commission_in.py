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

    file_path = path / f'India Sales Order Data_{lastmonth_month}.xlsx'
    with open(file_path, "rb") as file:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{file_path.name}"')
        msg.attach(part)

    file_path2 = path / f'IN_AR Data_{lastmonth_month}.xlsx'
    with open(file_path2, "rb") as file:
        part2 = MIMEBase('application', "octet-stream")
        part2.set_payload(file.read())
        encoders.encode_base64(part2)
        part2.add_header('Content-Disposition', f'attachment; filename="{file_path2.name}"')
        msg.attach(part2)

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

def read_emails_commission_in_from_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def ar_data_extract(month):
    q_in = f"""
select extract(month from lead_date) as month, 
    org_id,
    assigned,
    t.user_name lead_owner,
    'Fresh' cpcat,
    sale_campaign cpname,
    sum(case when lead_status = 'approved' then 1 else 0 end) approved_count,
    count(distinct lead_id) lead_count,
    count(distinct so_id) so_count,
    count(distinct do_id) do_count, 
    sum(case when do_status = 'delivered' then 1 else 0 end) delivered_count, 
    sum(case when lead_status = 'approved' then 1 else 0 end) / count(distinct lead_id)::float as ar
    from data_master_raw dma 
    left join _dagster_staging__dim_agent_team t ON dma.geo::text = t.geo::text AND dma.assigned = t.user_id
    where lead_type = 'A'
    and country_code = 'IN'
    and date_trunc('month', lead_date) = '{month}'
    group by 1,2,3,4,5,6
    """
    conn = pg.connect(os.getenv("PG_LIBPQ_CONN_CONFIG"))
    cur = conn.cursor()
    my = pd.read_sql_query(q_in,conn)
    conn.commit()
    cur.close()
    conn.close()
    return my

def extract_data(month):
    q = f"""
select so_date::date, 
    org_id,
    so_id,
    assigned,
    t.user_name lead_owner,
    do_id,
    lead_id,
    so_amount amount, 
    lead_status,
    so_status,
    do_status,
    case when do_status = 'delivered' then 1 else 0 end is_delivered,
    lead_type,
    do_date::date,
    do_modify_date::date do_updatedate,
    sale_campaign cp_name,
    case when lead_type = 'A' then 'Fresh' else 'Resell' end cpcat,
    sale_campaign cpname,
    total_items no_quantity
    from data_master_raw dma 
    left join _dagster_staging__dim_agent_team t ON dma.geo::text = t.geo::text AND dma.assigned = t.user_id
    where country_code = 'IN'
    and so_status in ('validated', 'delay')
    and date_trunc('month', so_date) = '{month}'
    """
    conn = pg.connect(os.getenv("PG_LIBPQ_CONN_CONFIG"))
    cur = conn.cursor()
    df = pd.read_sql_query(q,conn)
    conn.commit()
    cur.close()
    conn.close()
    df['Is_Reseller'] = np.where(df.no_quantity >= 100, 1, 0)
    return df


@asset(group_name="send_commission_email_in")
def sending_email_commission_in(context: AssetExecutionContext) -> None:
    today = date.today()
    lastmonth_date = (today.replace(day=1) + relativedelta(months=-2)).strftime("%Y-%m-%d")
    lastmonth_month = (today.replace(day=1) + relativedelta(months=-2)).strftime("%b-%y")
    df = extract_data(lastmonth_date)
    df.to_excel(os.path.join(path, f"India Sales Order Data_{lastmonth_month}.xlsx"))    
    df = ar_data_extract(lastmonth_date)
    df.to_excel(os.path.join(path, f"IN_AR Data_{lastmonth_month}.xlsx"))
    path_to = path / "ToCommissionMonthly_IN.txt"
    recipient = read_emails_commission_in_from_file(path_to)
    path_cc = path / "CcCommissionMonthly_IN.txt"
    ccRecipient = read_emails_commission_in_from_file(path_cc)
    subject = 'India Commission Raw Data for '+lastmonth_month
    message = '''Dear all,
    Please find attached to this email the Excel file of India Sales data & AR Data for the month.
    Thanks & Best Regards,'''
    send_mail(recipient,ccRecipient, subject, message,lastmonth_month)


send_email_commission_in_jobs = define_asset_job(
    name="send_email_commission_in_jobs",
    selection=AssetSelection.groups("send_commission_email_in"),
)

send_email_commission_in_schedule = ScheduleDefinition(
    job=send_email_commission_in_jobs,
    cron_schedule= "0 6 1 * *", 
    execution_timezone="Asia/Bangkok",
)
