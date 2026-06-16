"""Shared RatingChange fake for the versus tests.

Split out of ``test_versus`` so the pure-computation and DB/integration test
modules can each stay under the 500-line limit.
"""
import collections

# A lightweight RatingChange-like namedtuple for testing
RatingChange = collections.namedtuple(
    'RatingChange',
    'contestId contestName handle rank ratingUpdateTimeSeconds oldRating newRating'
)


def _make_rc(contest_id, handle, rank, update_time=None):
    """Helper to create a minimal RatingChange for testing."""
    return RatingChange(
        contestId=contest_id,
        contestName=f'Contest {contest_id}',
        handle=handle,
        rank=rank,
        ratingUpdateTimeSeconds=(
            update_time if update_time is not None else 1000000 + contest_id),
        oldRating=1500,
        newRating=1500,
    )
