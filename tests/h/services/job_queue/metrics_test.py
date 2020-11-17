import datetime
from unittest import mock

import pytest
from h_matchers import Any

from h.services.job_queue.metrics import JobQueueMetrics, factory


class TestJobQueue:
    def test_metrics_queue_length(self, factories, job_queue_metrics):
        now = datetime.datetime.utcnow()
        one_minute = datetime.timedelta(minutes=1)

        class JobFactory(factories.Job):
            name = "name_1"
            scheduled_at = now - one_minute
            priority = 1
            tag = "tag_1"

        JobFactory()
        JobFactory(name="name_2", scheduled_at=now + one_minute)
        JobFactory(tag="tag_2")
        JobFactory(priority=2)
        JobFactory(expires_at=now - one_minute)

        metrics = job_queue_metrics.metrics()

        assert (
            metrics
            == Any.list.containing(
                [
                    ("Custom/JobQueue/Count", 4),
                    ("Custom/JobQueue/Expired/Count", 1),
                    ("Custom/JobQueue/Name/name_1/Total/Count", 3),
                    ("Custom/JobQueue/Name/name_1/Tag/tag_1/Count", 2),
                    ("Custom/JobQueue/Name/name_1/Tag/tag_2/Count", 1),
                    ("Custom/JobQueue/Name/name_2/Total/Count", 1),
                    ("Custom/JobQueue/Name/name_2/Tag/tag_1/Count", 1),
                    ("Custom/JobQueue/Priorities/1/Count", 3),
                    ("Custom/JobQueue/Priorities/2/Count", 1),
                ]
            ).only()
        )

    @pytest.fixture
    def job_queue_metrics(self, db_session):
        return JobQueueMetrics(db_session)


class TestFactory:
    def test_it(self, pyramid_request):
        factory(mock.sentinel.context, pyramid_request)
