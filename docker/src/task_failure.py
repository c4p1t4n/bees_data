import boto3
from botocore.exceptions import ClientError
import logging
import os
# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Template de e-mail genérico
EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Erro no Pipeline</title>
</head>
<body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 30px;">
  <div style="max-width: 600px; margin: auto; background-color: #ffffff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
    <h2 style="color: #dc3545; text-align: center;">Erro no Pipeline</h2>
    <p style="font-size: 16px; color: #333333; text-align: center;">
      Um erro ocorreu durante a execução de um processo de dados.
    </p>
    <p style="font-size: 14px; color: #666666; text-align: center;">
      Verifique os logs para mais informações.
    </p>
    <div style="text-align: center; margin-top: 30px;">
      <a href="{log_url}" style="background-color: #007bff; color: #ffffff; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Ver Logs</a>
    </div>
  </div>
</body>
</html>
"""

def send_email_via_ses(subject, html_content, to_email, from_email, region='us-east-1'):
    ses_client = boto3.client(
        'ses',
        region_name=region,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID_EMAIL', ''),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY_EMAIL', ''),
    )
    try:
        logger.info("Tentando enviar e-mail via SES...")
        response = ses_client.send_email(
            Source=from_email,
            Destination={'ToAddresses': [to_email]},
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {'Html': {'Data': html_content, 'Charset': 'UTF-8'}}
            }
        )
        logger.info(f"E-mail enviado com sucesso! Message ID: {response['MessageId']}")
    except ClientError as e:
        logger.error(f"Erro ao enviar e-mail: {e.response['Error']['Message']}")
    except Exception as e:
        logger.exception("Erro inesperado ao enviar e-mail")

# Callback para Airflow (sem usar context)
def task_failure_callback():
    html = EMAIL_TEMPLATE.format(
        log_url="http://localhost:8080/logs"  # URL de exemplo, substitua conforme necessário
    )
    send_email_via_ses(
        subject="Falha na execução de pipeline",
        html_content=html,
        to_email="leoigornu@gmail.com",
        from_email="leoigornunes@gmail.com"
    )

# Teste manual (opcional)
def testar_envio():
    html = EMAIL_TEMPLATE.format(
        log_url="http://localhost:8080/logs"
    )
    send_email_via_ses(
        subject="[TESTE] Falha na execução de pipeline",
        html_content=html,
        to_email="leoigornu@gmail.com",
        from_email="leoigornunes@gmail.com"
    )

# Descomente abaixo para testar localmente
testar_envio()
