import psycopg2
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from mconfig import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAILS

def create_html_table(df):
    # Create an HTML table from the DataFrame
    html_table = df.to_html(index=False)

    # Highlight rows where MB_behind is greater than 65 MB
    highlighted_rows = []
    for _, row in df.iterrows():
        if row['MB_behind'] > 65:
            highlighted_rows.append('<tr style="background-color: #ffcccc;">')
        else:
            highlighted_rows.append('<tr>')

    html_table = html_table.replace('<table>', '<table style="border-collapse: collapse;">')
    html_table = html_table.replace('<th>', '<th style="border: 1px solid black; padding: 8px; background-color: #f2f2f2;">')
    html_table = html_table.replace('<td>', '<td style="border: 1px solid black; padding: 8px;">')
    html_table = html_table.replace('<tr>', '\n'.join(highlighted_rows))

    return html_table

def execute_postgres_query_and_send_email():
    # PostgreSQL query
    query = "SELECT slot_name, round((redo_lsn - restart_lsn) / 1024 / 1024, 2) AS MB_behind FROM pg_control_checkpoint(), pg_replication_slots;"

    # Establish a connection to PostgreSQL
    try:
        connection = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cursor = connection.cursor()

        # Execute the query and fetch the result
        cursor.execute(query)
        result = cursor.fetchall()

        # Convert the result to a pandas DataFrame
        columns = ['slot_name', 'MB_behind']
        df = pd.DataFrame(result, columns=columns)

        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error occurred while connecting to the database or executing the query: {e}")
        return

    # Prepare the email content with the HTML table
    email_subject = 'Replication Slot Size Result'
    html_table = create_html_table(df)
    email_body = f"<html><body><h2>{email_subject}</h2>{html_table}</body></html>"

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ', '.join(RECEIVER_EMAILS)  # Join the list of email addresses into a comma-separated string
    msg['Subject'] = email_subject

    # Attach the HTML table to the email
    msg.attach(MIMEText(email_body, 'html'))

    # Establish the SMTP connection and send the email
    try:
        smtp_server = 'smtp.office365.com'  # Change this to your Outlook SMTP server
        smtp_port = 587  # Change this to the appropriate port

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Upgrade the connection to a secure SSL/TLS connection
        server.login(SENDER_EMAIL, SENDER_PASSWORD)

        # Send the email
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAILS, msg.as_string())

        # Close the connection
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error occurred while sending email: {e}")

if __name__ == "__main__":
    execute_postgres_query_and_send_email()