"""Tests for starboard leaderboard DB methods."""
import pytest

from tests.test_starboard_db import FakeUserDb, GUILD, STAR, FIRE, THUMBS_UP


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Leaderboards
# =====================================================================

class TestLeaderboards:
    def _seed_messages(self, db):
        """Create test data: user1 has 3 messages, user2 has 1."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        for i in range(3):
            db.add_starboard_message_v1(f'msg{i}', f'sb{i}', GUILD, STAR, author_id='user1')
            db.update_starboard_star_count(f'msg{i}', STAR, 5)
        db.add_starboard_message_v1('msg10', 'sb10', GUILD, STAR, author_id='user2')
        db.update_starboard_star_count('msg10', STAR, 10)

    def test_message_leaderboard(self, db):
        self._seed_messages(db)
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 2
        assert rows[0].author_id == 'user1'
        assert rows[0].message_count == 3
        assert rows[1].author_id == 'user2'
        assert rows[1].message_count == 1

    def test_star_leaderboard(self, db):
        self._seed_messages(db)
        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 2
        # user1: 3 messages * 5 stars = 15 total
        # user2: 1 message * 10 stars = 10 total
        assert rows[0].author_id == 'user1'
        assert rows[0].total_stars == 15
        assert rows[1].author_id == 'user2'
        assert rows[1].total_stars == 10

    def test_leaderboard_excludes_null_author(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)  # No author_id
        db.update_starboard_star_count('msg1', STAR, 5)
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 0
        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 0

    def test_leaderboard_excludes_unknown_sentinel(self, db):
        """Bug #10 fix: __UNKNOWN__ sentinel should be excluded from leaderboards."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 5)
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR)
        db.update_starboard_author_and_count('msg2', STAR, '__UNKNOWN__', 0)

        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'

        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 1

    def test_leaderboard_empty(self, db):
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert rows == []

    def test_leaderboard_per_emoji(self, db):
        """Leaderboard should only include messages for the queried emoji."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 3, 0xff0000)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, FIRE, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 5)
        db.update_starboard_star_count('msg2', FIRE, 10)

        star_lb = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(star_lb) == 1
        assert star_lb[0].message_count == 1

        fire_lb = db.get_starboard_star_leaderboard(GUILD, FIRE)
        assert len(fire_lb) == 1
        assert fire_lb[0].total_stars == 10


class TestTopMessagesByUser:
    def _seed(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 10)
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR, author_id='user1')
        db.update_starboard_star_count('msg2', STAR, 5)
        db.add_starboard_message_v1('msg3', 'sb3', GUILD, STAR, author_id='user2')
        db.update_starboard_star_count('msg3', STAR, 20)

    def test_filter_by_author(self, db):
        self._seed(db)
        rows = db.get_top_starboard_messages(GUILD, STAR, author_id='user1')
        assert len(rows) == 2
        assert all(r.author_id == 'user1' for r in rows)
        assert rows[0].star_count == 10
        assert rows[1].star_count == 5

    def test_no_filter_returns_all(self, db):
        self._seed(db)
        rows = db.get_top_starboard_messages(GUILD, STAR)
        assert len(rows) == 3

    def test_filter_nonexistent_user(self, db):
        self._seed(db)
        rows = db.get_top_starboard_messages(GUILD, STAR, author_id='nobody')
        assert rows == []


class TestStarGiversWithAliases:
    def test_alias_reactors_counted_in_star_givers(self, db):
        """Star givers leaderboard should include users who reacted with aliases."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='author1')
        db.add_reactor('msg1', THUMBS_UP, 'reactor1')  # reacted with alias only
        db.add_reactor('msg1', STAR, 'reactor2')        # reacted with main

        family = db.get_emoji_family(GUILD, STAR)
        rows = db.get_star_givers_leaderboard(GUILD, STAR, emoji_family=family)
        givers = {r.user_id: r.stars_given for r in rows}
        assert 'reactor1' in givers
        assert 'reactor2' in givers

    def test_star_givers_no_double_count_same_message(self, db):
        """User who reacted with both main and alias on same message counts as 1 star given."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='author1')
        db.add_reactor('msg1', STAR, 'reactor1')
        db.add_reactor('msg1', THUMBS_UP, 'reactor1')  # same user, same msg

        family = db.get_emoji_family(GUILD, STAR)
        rows = db.get_star_givers_leaderboard(GUILD, STAR, emoji_family=family)
        assert len(rows) == 1
        assert rows[0].stars_given == 1  # not 2

    def test_without_family_misses_alias_reactors(self, db):
        """Without emoji_family, alias reactors are not counted (backward compat)."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='author1')
        db.add_reactor('msg1', THUMBS_UP, 'reactor1')

        rows = db.get_star_givers_leaderboard(GUILD, STAR)  # no family
        assert len(rows) == 0  # alias reactor missed

    def test_without_family_still_counts_main_reactors(self, db):
        """Without emoji_family, main-emoji reactors still work correctly."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='author1')
        db.add_reactor('msg1', STAR, 'reactor1')
        db.add_reactor('msg1', STAR, 'reactor2')

        rows = db.get_star_givers_leaderboard(GUILD, STAR)  # no family
        assert len(rows) == 2


class TestNarcissusWithAliases:
    def test_self_star_via_alias_counted(self, db):
        """Narcissus leaderboard should count self-stars via aliases."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')  # self-star via alias

        family = db.get_emoji_family(GUILD, STAR)
        rows = db.get_narcissus_leaderboard(GUILD, STAR, emoji_family=family)
        assert len(rows) == 1
        assert rows[0].user_id == 'user1'

    def test_narcissus_no_double_count(self, db):
        """Self-star via both main and alias on same message counts as 1."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')

        family = db.get_emoji_family(GUILD, STAR)
        rows = db.get_narcissus_leaderboard(GUILD, STAR, emoji_family=family)
        assert len(rows) == 1
        assert rows[0].self_stars == 1  # not 2
