import json
import logging
import smtplib
from email.message import EmailMessage
from email.utils import formataddr

# Load configuration file
with open('config.json', 'r') as fp:
    conf = json.load(fp)


def send_email(e):
    # Send an email
    email_config = conf['email']
    sender = email_config['email_address']
    recipient = sender
    smtp_server = email_config['smtp_server']
    smtp_port = email_config['smtp_port']
    username = email_config['username']
    password = email_config['password']

    msg = EmailMessage()
    msg['From'] = formataddr(('Internet Yellow Pages.', f'{sender}'))
    msg['To'] = recipient
    msg['Subject'] = 'Error when running create_db script'
    msg.set_content(
        f"""\
        An exception was caught during run of create_db script:\n\n{str(e)}
        """
    )
    msg.add_alternative(
        f"""\
        <html>
          <body>
            <p>An exception was caught during run of create_db script:</p>
            <p>{str(e)}</p>
          </body>
        </html>
        """,
        subtype="html",
    )

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(username, password)
        server.sendmail(sender, recipient, msg.as_string())
        logging.info("Email sent")


if __name__ == "__main__":
    send_email("Error when running Crawler 6")
