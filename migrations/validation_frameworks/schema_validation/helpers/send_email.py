import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

# TURN ON OPTION " ALLOW LESS SECURE APPS ", OTHERWISE IT WOULDN'T WORK

emailfrom = "noumanaliravian@gmail.com"
emailto = "naman.ali@northbaysolutions.net"
fileToSend = "/Users/nouma/PycharmProjects/nurix-test-framework/test-framework/Reports/allure-report/data/suites.csv"
username = "noumanaliravian@gmail.com"
password = "abcdefgh"

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = emailto
msg["Subject"] = "Schema Validation Report"
msg.preamble = "Did not get this"

ctype, encoding = mimetypes.guess_type(fileToSend)
if ctype is None or encoding is not None:
    ctype = "application/octet-stream"

maintype, subtype = ctype.split("/", 1)

if maintype == "text":
    fp = open(fileToSend)
    # Note: we should handle calculating the charset
    attachment = MIMEText(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "image":
    fp = open(fileToSend, "rb")
    attachment = MIMEImage(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "audio":
    fp = open(fileToSend, "rb")
    attachment = MIMEAudio(fp.read(), _subtype=subtype)
    fp.close()
else:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
msg.attach(attachment)

server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
try:
    server.login(username, password)
    server.sendmail(emailfrom, emailto, msg.as_string())
finally:
    server.quit()