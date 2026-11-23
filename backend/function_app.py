import json

import azure.functions as func

from main import app as fastapi_app
from main import run_bot_match


app = func.AsgiFunctionApp(
    app=fastapi_app,
    http_auth_level=func.AuthLevel.ANONYMOUS,
)


@app.queue_trigger(
    arg_name='message',
    queue_name='mindclash-bot-matches',
    connection='AzureWebJobsStorage',
)
async def process_bot_match(message: func.QueueMessage):
    job = json.loads(message.get_body().decode('utf-8'))
    await run_bot_match(
        job['match_id'],
        int(job['bot_id']),
        int(job['player_rating']),
    )
