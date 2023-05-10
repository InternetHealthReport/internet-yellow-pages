import json
import logging
import smtplib
from email.message import EmailMessage
from email.utils import formataddr

# Load configuration file
with open('config.json', 'r') as fp:
    conf = json.load(fp)


def send_email(e):
    email_config = conf.get('email')
    if email_config is not None and email_config['email_address'] != "" and email_config['smtp_server'] != "" and email_config['smtp_port'] != "" and email_config['username'] != "" and email_config['password'] != "":
        # Send an email
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
            server.quit()
    else:
        print('Email credentials not found in config file')


if __name__ == "__main__":
    send_email("Error when running Crawler 6")
