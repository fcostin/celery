import logging

import pytz
import six
import tzcron
from kombu.utils import cached_property
from pytz import AmbiguousTimeError, NonExistentTimeError

from . import schedules
from .utils.time import is_naive


logger = logging.getLogger(__name__)


class tzcrontab(schedules.BaseSchedule):
    def __init__(self, expression, tz=None, *args, **kwargs):
        """
        Crontab schedule supporting a timezone.

        Expression parsing and generation of event times is using `tzcron`:
        https://tzcron.readthedocs.io

        :param expression: cron expression (with year), see `tzcron` docs for details
        :param tz: timezone as string (eg. 'Europe/Vienna') or `pytz.timezone`
        """
        self.expression = expression
        # set 'timezone' attribute like on `self.app`, use `self.tz` property to retrieve
        if isinstance(tz, six.text_type):
            self.timezone = pytz.timezone(tz)
        else:
            self.timezone = tz
        super(tzcrontab, self).__init__(**kwargs)

    @cached_property
    def tz(self):
        return self.timezone or self.app.timezone

    def __repr__(self):
        template = "<{}: {} @{}>"
        return template.format(self.__class__.__name__, self.expression, self.tz)

    def __eq__(self, other):
        if isinstance(other, tzcrontab):
            return all([
                other.expression == self.expression,
                other.timezone == self.timezone,
                super(tzcrontab, self).__eq__(other)
            ])
        return NotImplemented

    def now(self):
        now = super(tzcrontab, self).now()
        assert not is_naive(now), "Please, don't use naive datetimes!"
        return now

    def remaining_estimate(self, last_run_at):
        # make a schedule from the cron expression, starting now
        # any events we might have missed since `last_run_at` are ignored
        now = self.now()
        event_datetimes = tzcron.Schedule(self.expression, self.tz, now)
        logger.debug('new schedule from cron expression: %s', event_datetimes)

        try:
            # find the next event
            next_datetime = next(event_datetimes)
        except AmbiguousTimeError:
            logger.exception(
                "Time is ambiguous in the requested timezone! "
                "Task will not be scheduled!"
            )
            return
        except NonExistentTimeError:
            logger.exception(
                "Time does not exist in the requested timezone! "
                "Task will not be scheduled!"
            )
            return

        remaining_delta = next_datetime - now
        logger.debug(
            'remaining_estimate: %s, next_datetime: %s, remaining_delta: %s',
            self.tz, next_datetime, remaining_delta,
        )
        return remaining_delta

    def is_due(self, last_run_at):
        """
        Return tuple of `(is_due, remaining_delta)`.
        """
        remaining_delta = self.remaining_estimate(last_run_at)
        if remaining_delta:
            remaining_seconds = max(remaining_delta.total_seconds(), 0)
            is_due = remaining_seconds == 0
            logger.debug('is_due: %s, remaining: %s', is_due, remaining_seconds)
            return is_due, remaining_seconds

        # when determined time was ambiguous or non-existent
        return False, 0


class pytzcrontab(tzcrontab):
    def __init__(self, minute='*', hour='*', day_of_week='*',
                 day_of_month='*', month_of_year='*', year='*',
                 tz=None, *args, **kwargs):
        """
        Wrapper for `tzcrontab` with more "pythonic" interface, that allows passing
        the parts of a cron expression as separate arguments.

        :param minute: 0-59 or pattern, default: '*'
        :param hour: 0-23 or pattern, default: '*'
        :param day_of_week: 1-7 (Monday to Sunday) or pattern, default: '*'
        :param day_of_month: 1-31 or pattern, default: '*'
        :param month_of_year: 1-12 or pattern, default: '*'
        :param year: full year (yyyy) or '*', default: '*'
        :param tz: timezone as string (eg. 'Europe/Vienna') or `pytz.timezone`
        """
        expression = ' '.join([
            str(arg) for arg
            in (minute, hour, day_of_month, month_of_year, day_of_week, year)
        ])
        super(pytzcrontab, self).__init__(expression, tz=tz, *args, **kwargs)
        
