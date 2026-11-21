import asyncio
import os
import secrets
import unittest
from unittest.mock import AsyncMock, patch

with patch.dict(os.environ, JWT_SECRET=secrets.token_hex(32),
                DATABASE_URL='sqlite://', REDIS_URL='redis://localhost/0'):
    import main


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.session = main.SessionLocal()
        self.addCleanup(self.session.close)
        self.player, self.peer, self.bot = [
            main.User(username=name, email=name, password_hash='unused')
            for name in ('player', 'peer', 'MindClash_AI')
        ]
        self.session.add_all([self.player, self.peer, self.bot])
        self.session.flush()

    def match(self, peer):
        match = main.Match(id=secrets.token_hex(8), player1=self.player.id,
                           player2=peer.id, arena='logic', score1=10)
        self.session.add(match)
        self.session.flush()
        return match

    def test_only_human_matches_count_towards_rank(self):
        main.finish_match_if_needed(self.match(self.bot), self.session)
        self.assertEqual((self.player.rating, self.player.wins, self.bot.losses), (1000, 0, 0))
        main.finish_match_if_needed(self.match(self.peer), self.session)
        self.assertEqual((self.player.rating, self.player.wins, self.peer.losses), (1016, 1, 1))

    def test_ticket_scope_and_consumption(self):
        match = self.match(self.peer)
        store = {}
        with patch.object(main, 'redis_client') as cache:
            def save(key, value, ex):
                self.assertEqual(ex, 30)
                store[key] = value
            cache.set.side_effect = save
            cache.getdel.side_effect = lambda key: store.pop(key, None)
            with self.assertRaises(main.HTTPException) as denied:
                main.socket_ticket(match.id, self.bot, self.session)
            self.assertEqual(denied.exception.status_code, 404)
            ticket = main.socket_ticket(match.id, self.player, self.session)['ticket']
            ws = AsyncMock()
            asyncio.run(main.match_socket(ws, 'other', ticket))
            ws.close.assert_awaited_once_with(code=4401)
            ws.reset_mock()
            ws.receive_text.side_effect = main.WebSocketDisconnect()
            with patch.object(main, 'SessionLocal', return_value=self.session), patch.object(self.session, 'close'):
                asyncio.run(main.match_socket(ws, match.id, ticket))
            ws.accept.assert_awaited_once()
            asyncio.run(main.match_socket(ws, match.id, ticket))
            ws.close.assert_awaited_once_with(code=4401)
