import datetime
import logging

import pytz
import six
import tzcron
from kombu.utils import cached_property
from pytz import AmbiguousTimeError, NonExistentTimeError

from . import schedules
from .utils.time import is_naive


logger = logging.getLogger(__name__)

def astimezone(x):
    if isinstance(x, datetime.tzinfo):
        return x
    # attempt to coerce
    return pytz.timezone(x)

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
        return self.timezone or astimezone(self.app.timezone)

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

        assert not is_naive(last_run_at), "Please, don't use naive datetimes!"

        now = self.now()

        # We are only interested in the next event strictly greater than the
        # last_run_at time.
        start_date = max(now, last_run_at + datetime.timedelta(seconds=1))

        event_datetimes = tzcron.Schedule(
            expression = self.expression,
            t_zone = self.tz,
            start_date = start_date,
        )
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
        Return tuple of `(due, remaining_delta)`.
        """

        # assert not is_naive(last_run_at), "Please, don't use naive datetimes!"

        remaining_delta = self.remaining_estimate(last_run_at)
        if remaining_delta is None:
            # when determined time was ambiguous or non-existent
            return schedules.schedstate(False, 0) # should this be False, INF ?

        remaining_seconds = max(remaining_delta.total_seconds(), 0)
        due = remaining_seconds == 0
        logger.debug('due: %s, remaining: %s', due, remaining_seconds)
        if due:
            rem_delta = self.remaining_estimate(self.now())
            remaining_seconds = max(rem_delta.total_seconds(), 0)
        return schedules.schedstate(due, remaining_seconds)


def _expand_token(arg):
    if isinstance(arg, (list, tuple)):
        return ','.join(map(str, arg))
    else:
        return str(arg)


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
            _expand_token(arg) for arg
            in (minute, hour, day_of_month, month_of_year, day_of_week, year)
        ])

        self.hour = schedules.crontab._expand_cronspec(hour, 24)
        self.minute = schedules.crontab._expand_cronspec(minute, 60)
        self.day_of_week = schedules.crontab._expand_cronspec(day_of_week, 7)
        self.day_of_month = schedules.crontab._expand_cronspec(day_of_month, 31, 1)
        self.month_of_year = schedules.crontab._expand_cronspec(month_of_year, 12, 1)

        super(pytzcrontab, self).__init__(expression, tz=tz, *args, **kwargs)
        
