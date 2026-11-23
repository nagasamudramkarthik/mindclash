import asyncio
import base64
import json
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch

with patch.dict(os.environ, JWT_SECRET='f' * 64, DATABASE_URL='sqlite://',
                REDIS_URL='redis://localhost:6379/0'):
    import function_app
    import main


class FunctionQueueTests(unittest.TestCase):
    def test_bot_match_job_is_enqueued_for_the_function_worker(self):
        client = Mock()
        client.create_queue.side_effect = main.ResourceExistsError('already exists')
        with patch.dict(os.environ, {'AzureWebJobsStorage': 'local-storage'}), \
                patch.object(main.QueueClient, 'from_connection_string', return_value=client) as factory:
            main.enqueue_bot_match('match-id', 7, 1200)

        factory.assert_called_once_with('local-storage', 'mindclash-bot-matches')
        payload = json.loads(base64.b64decode(client.send_message.call_args.args[0]))
        self.assertEqual(payload, {
            'match_id': 'match-id',
            'bot_id': 7,
            'player_rating': 1200,
        })
        client.close.assert_called_once()

    def test_queue_trigger_runs_the_bot_worker(self):
        message = Mock()
        message.get_body.return_value = b'{"match_id":"match-id","bot_id":"7","player_rating":"1200"}'
        worker = AsyncMock()
        with patch.object(function_app, 'run_bot_match', worker):
            asyncio.run(function_app.process_bot_match(message))

        worker.assert_awaited_once_with('match-id', 7, 1200)

    def test_health_requires_database_and_redis(self):
        connection = Mock()
        context = Mock()
        context.__enter__ = Mock(return_value=connection)
        context.__exit__ = Mock(return_value=False)
        with patch.object(main.redis_client, 'ping') as ping, \
                patch.object(main.engine, 'connect', return_value=context):
            self.assertEqual(main.health(), {
                'status': 'ok',
                'redis': 'ok',
                'database': 'ok',
            })

        ping.assert_called_once()
        connection.execute.assert_called_once()

    def test_health_reports_database_failures(self):
        with patch.object(main.redis_client, 'ping'), \
                patch.object(main.engine, 'connect', side_effect=main.SQLAlchemyError('unavailable')):
            with self.assertRaises(main.HTTPException) as unavailable:
                main.health()

        self.assertEqual(unavailable.exception.status_code, 503)
