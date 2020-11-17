from datetime import datetime

from sqlalchemy import func

from h.models import Job


class JobQueueMetrics:
    """A service for generating metrics about the job queue."""

    def __init__(self, db):
        self._db = db

    def metrics(self):
        """
        Return a list of New Relic-style metrics about the job queue.

        The returned list of metrics is suitable for passing to
        newrelic.agent.record_custom_metrics().
        """
        metrics = []
        now = datetime.utcnow()

        # Expired jobs.
        metrics.append(
            (
                "Custom/JobQueue/Expired/Count",
                self._db.query(Job).filter(Job.expires_at < now).count(),
            )
        )

        # Unexpired jobs by tag and name.
        names = (row.name for row in self._db.query(Job.name).distinct().all())
        for name in names:
            tag_counts = (
                self._db.query(Job.tag, func.count(Job.id))
                .filter(Job.expires_at >= now, Job.name == name)
                .group_by(Job.tag)
                .all()
            )

            for tag, count in tag_counts:
                metrics.append((f"Custom/JobQueue/Name/{name}/Tag/{tag}/Count", count))

            metrics.append(
                (
                    f"Custom/JobQueue/Name/{name}/Total/Count",
                    sum(count for _, count in tag_counts),
                )
            )

        # Unexpired jobs by priority.
        priority_counts = (
            self._db.query(Job.priority, func.count(Job.id))
            .filter(Job.expires_at >= now)
            .group_by(Job.priority)
            .all()
        )
        for priority, count in priority_counts:
            metrics.append((f"Custom/JobQueue/Priorities/{priority}/Count", count))

        # Total unexpired jobs.
        metrics.append(
            ("Custom/JobQueue/Count", sum(count for _, count in priority_counts))
        )

        return metrics


def factory(_context, request):
    return JobQueueMetrics(request.db)
