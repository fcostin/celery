from __future__ import absolute_import, unicode_literals

import time
from contextlib import contextmanager
from datetime import timedelta
from datetime import datetime
from pickle import dumps, loads

import pytest
import pytz
from case import Case, Mock, skip

from celery.five import items
from celery.schedules_tzcron import pytzcrontab
import celery.utils.time

assertions = Case('__init__')

DEFAULT_TZ = pytz.utc


@contextmanager
def patch_crontab_nowfun(cls, retval):
    prev_nowfun = cls.nowfun
    cls.nowfun = lambda: retval
    try:
        yield
    finally:
        cls.nowfun = prev_nowfun


def is_time_feasible_wrt_crontab_schedule(t, z):
    # z : celery.schedules.crontab instance
    t = z.maybe_make_aware(t)
    return (
        t.month in z.month_of_year and
        (t.isoweekday() % 7) in z.day_of_week and
        t.day in z.day_of_month and
        t.hour in z.hour and
        t.minute in z.minute
    )


def datetime_utc(*args, **kwargs):
    t = datetime(*args, **kwargs)
    return celery.utils.time.make_aware(t, DEFAULT_TZ)


class test_crontab_remaining_estimate:

    def pytzcrontab(self, *args, **kwargs):
        return pytzcrontab(*args, **dict(kwargs, app=self.app))

    def next_ocurrance(self, crontab, now):
        crontab.nowfun = lambda: now
        return now + crontab.remaining_estimate(now)

    def test_next_minute(self):
        next = self.next_ocurrance(
            self.pytzcrontab(), datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 11, 14, 31)

    def test_not_next_minute(self):
        next = self.next_ocurrance(
            self.pytzcrontab(), datetime_utc(2010, 9, 11, 14, 59, 15),
        )
        assert next == datetime_utc(2010, 9, 11, 15, 0)

    def test_this_hour(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42]), datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 11, 14, 42)

    def test_not_this_hour(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 10, 15]),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 11, 15, 5)

    def test_today(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], hour=[12, 17]),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 11, 17, 5)

    def test_not_today(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], hour=[12]),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 12, 12, 5)

    def test_weekday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_week='sat'),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 18, 14, 30)

    def test_not_weekday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon-fri'),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 13, 0, 5)

    def test_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_month=18),
            datetime_utc(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 9, 18, 14, 30)

    def test_not_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_month=29),
            datetime_utc(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 1, 29, 0, 5)

    def test_weekday_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14,
                         day_of_week='mon', day_of_month=18),
            datetime_utc(2010, 1, 18, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 10, 18, 14, 30)

    def test_monthday_not_weekday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='sat', day_of_month=29),
            datetime_utc(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 5, 29, 0, 5)

    def test_weekday_not_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime_utc(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 1, 18, 0, 5)

    def test_not_weekday_not_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime_utc(2010, 1, 10, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 1, 18, 0, 5)

    def test_leapday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_month=29),
            datetime_utc(2012, 1, 29, 14, 30, 15),
        )
        assert next == datetime_utc(2012, 2, 29, 14, 30)

    def test_not_leapday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_month=29),
            datetime_utc(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 3, 29, 14, 30)

    def test_weekmonthdayyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year=1),
            datetime_utc(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 1, 29, 14, 30)

    def test_monthdayyear_not_week(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='wed,thu',
                         day_of_month=29, month_of_year='1,4,7'),
            datetime_utc(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 4, 29, 0, 5)

    def test_weekdaymonthyear_not_monthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year='1-10'),
            datetime_utc(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 10, 29, 14, 30)

    def test_weekmonthday_not_monthyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='fri',
                         day_of_month=29, month_of_year='2-10'),
            datetime_utc(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 10, 29, 0, 5)

    def test_weekday_not_monthdayyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=18, month_of_year='2-10'),
            datetime_utc(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 10, 18, 0, 5)

    def test_monthday_not_weekdaymonthyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime_utc(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 3, 29, 0, 5)

    def test_monthyear_not_weekmonthday(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime_utc(2010, 2, 28, 0, 5, 15),
        )
        assert next == datetime_utc(2010, 3, 29, 0, 5)

    def test_not_weekmonthdayyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=[5, 42], day_of_week='fri,sat',
                         day_of_month=29, month_of_year='2-10'),
            datetime_utc(2010, 1, 28, 14, 30, 15),
        )
        assert next == datetime_utc(2010, 5, 29, 0, 5)

    @pytest.mark.skip("broken - pytzcrontab goes into an infinite loop here")
    def test_invalid_specification(self):
        # *** WARNING ***
        # This test triggers an infinite loop in case of a regression
        with pytest.raises(RuntimeError):
            self.next_ocurrance(
                self.pytzcrontab(day_of_month=31, month_of_year=4),
                datetime_utc(2010, 1, 28, 14, 30, 15),
            )

    def test_leapyear(self):
        next = self.next_ocurrance(
            self.pytzcrontab(minute=30, hour=14, day_of_month=29, month_of_year=2),
            datetime_utc(2012, 2, 29, 14, 30),
        )
        assert next == datetime_utc(2016, 2, 29, 14, 30)

    def test_day_after_dst_end(self):
        # Test for #1604 issue with region configuration using DST
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = pytz.timezone(tzname)
        crontab = self.pytzcrontab(minute=0, hour=9)

        # Set last_run_at Before DST end
        last_run_at = tz.localize(datetime(2017, 10, 28, 9, 0))
        # Set now after DST end
        now = tz.localize(datetime(2017, 10, 29, 7, 0))
        crontab.nowfun = lambda: now
        next = now + crontab.remaining_estimate(last_run_at)

        assert next.utcoffset().seconds == 3600
        assert next == tz.localize(datetime(2017, 10, 29, 9, 0))

    def test_day_after_dst_start(self):
        # Test for #1604 issue with region configuration using DST
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = pytz.timezone(tzname)
        crontab = self.pytzcrontab(minute=0, hour=9)

        # Set last_run_at Before DST start
        last_run_at = tz.localize(datetime(2017, 3, 25, 9, 0))
        # Set now after DST start
        now = tz.localize(datetime(2017, 3, 26, 7, 0))
        crontab.nowfun = lambda: now
        next = now + crontab.remaining_estimate(last_run_at)

        assert next.utcoffset().seconds == 7200
        assert next == tz.localize(datetime(2017, 3, 26, 9, 0))


class test_crontab_is_due:

    def setup(self):
        self.now = self.app.now()
        self.next_minute = 60 - self.now.second - 1e-6 * self.now.microsecond
        self.every_minute = self.pytzcrontab()
        self.quarterly = self.pytzcrontab(minute='*/15')
        self.hourly = self.pytzcrontab(minute=30)
        self.daily = self.pytzcrontab(hour=7, minute=30)
        self.weekly = self.pytzcrontab(hour=7, minute=30, day_of_week='thursday')
        self.monthly = self.pytzcrontab(
            hour=7, minute=30, day_of_week='thursday', day_of_month='8-14',
        )
        self.monthly_moy = self.pytzcrontab(
            hour=22, day_of_week='*', month_of_year='2',
            day_of_month='26,27,28',
        )
        self.yearly = self.pytzcrontab(
            hour=7, minute=30, day_of_week='thursday',
            day_of_month='8-14', month_of_year=3,
        )

    def pytzcrontab(self, *args, **kwargs):
        return pytzcrontab(*args, app=self.app, **kwargs)

    def test_default_crontab_spec(self):
        c = self.pytzcrontab()
        assert c.minute == set(range(60))
        assert c.hour == set(range(24))
        assert c.day_of_week == set(range(7))
        assert c.day_of_month == set(range(1, 32))
        assert c.month_of_year == set(range(1, 13))

    def test_simple_crontab_spec(self):
        c = self.pytzcrontab(minute=30)
        assert c.minute == {30}
        assert c.hour == set(range(24))
        assert c.day_of_week == set(range(7))
        assert c.day_of_month == set(range(1, 32))
        assert c.month_of_year == set(range(1, 13))

    @pytest.mark.parametrize('minute,expected', [
        (30, {30}),
        ('30', {30}),
        ((30, 40, 50), {30, 40, 50}),
        ((30, 40, 50, 51), {30, 40, 50, 51})
    ])
    def test_crontab_spec_minute_formats(self, minute, expected):
        c = self.pytzcrontab(minute=minute)
        assert c.minute == expected

    @pytest.mark.parametrize('minute', [60, '0-100'])
    def test_crontab_spec_invalid_minute(self, minute):
        with pytest.raises(ValueError):
            self.pytzcrontab(minute=minute)

    @pytest.mark.parametrize('hour,expected', [
        (6, {6}),
        ('5', {5}),
        ((4, 8, 12), {4, 8, 12}),
    ])
    def test_crontab_spec_hour_formats(self, hour, expected):
        c = self.pytzcrontab(hour=hour)
        assert c.hour == expected

    @pytest.mark.parametrize('hour', [24, '0-30'])
    def test_crontab_spec_invalid_hour(self, hour):
        with pytest.raises(ValueError):
            self.pytzcrontab(hour=hour)

    @pytest.mark.parametrize('day_of_week,expected', [
        (5, {5}),
        ('5', {5}),
        ('fri', {5}),
        ('tuesday,sunday,fri', {0, 2, 5}),
        ('mon-fri', {1, 2, 3, 4, 5}),
        ('*/2', {0, 2, 4, 6}),
    ])
    def test_crontab_spec_dow_formats(self, day_of_week, expected):
        c = self.pytzcrontab(day_of_week=day_of_week)
        assert c.day_of_week == expected

    @pytest.mark.parametrize('day_of_week', [
        'fooday-barday', '1,4,foo', '7', '12',
    ])
    def test_crontab_spec_invalid_dow(self, day_of_week):
        with pytest.raises(ValueError):
            self.pytzcrontab(day_of_week=day_of_week)

    @pytest.mark.parametrize('day_of_month,expected', [
        (5, {5}),
        ('5', {5}),
        ('2,4,6', {2, 4, 6}),
        ('*/5', {1, 6, 11, 16, 21, 26, 31}),
    ])
    def test_crontab_spec_dom_formats(self, day_of_month, expected):
        c = self.pytzcrontab(day_of_month=day_of_month)
        assert c.day_of_month == expected

    @pytest.mark.parametrize('day_of_month', [0, '0-10', 32, '31,32'])
    def test_crontab_spec_invalid_dom(self, day_of_month):
        with pytest.raises(ValueError):
            self.pytzcrontab(day_of_month=day_of_month)

    @pytest.mark.parametrize('month_of_year,expected', [
        (1, {1}),
        ('1', {1}),
        ('2,4,6', {2, 4, 6}),
        ('*/2', {1, 3, 5, 7, 9, 11}),
        ('2-12/2', {2, 4, 6, 8, 10, 12}),
    ])
    def test_crontab_spec_moy_formats(self, month_of_year, expected):
        c = self.pytzcrontab(month_of_year=month_of_year)
        assert c.month_of_year == expected

    @pytest.mark.parametrize('month_of_year', [0, '0-5', 13, '12,13'])
    def test_crontab_spec_invalid_moy(self, month_of_year):
        with pytest.raises(ValueError):
            self.pytzcrontab(month_of_year=month_of_year)

    def seconds_almost_equal(self, a, b, precision):
        for index, skew in enumerate((+1, -1, 0)):
            try:
                assertions.assertAlmostEqual(a, b + skew, precision)
            except Exception as exc:
                # AssertionError != builtins.AssertionError in py.test
                if 'AssertionError' in str(exc):
                    if index + 1 >= 3:
                        raise
            else:
                break

    def test_every_minute_execution_is_due(self):
        now = datetime_utc(2019, 5, 3, 7, 24)
        last_ran = now - timedelta(seconds=60)
        next_minute = 60 - now.second - 1e-6 * now.microsecond
        with patch_crontab_nowfun(self.every_minute, now):
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, next_minute, 1)

    def test_every_minute_execution_is_not_due(self):
        now = self.now
        last_ran = now - timedelta(seconds=59)
        next_minute = 60 - now.second - 1e-6 * now.microsecond
        with patch_crontab_nowfun(self.every_minute, now):
            due, remaining = self.every_minute.is_due(last_ran)
            assert not due
            self.seconds_almost_equal(remaining, next_minute, 1)

    def test_execution_is_due_on_saturday(self):
        # 29th of May 2010 is a saturday
        now = datetime_utc(2010, 5, 29, 10, 30)
        last_ran = now - timedelta(seconds=61)
        next_minute = 60 - now.second - 1e-6 * now.microsecond
        with patch_crontab_nowfun(self.every_minute, now):
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_execution_is_due_on_sunday(self):
        # 30th of May 2010 is a sunday
        now = datetime_utc(2010, 5, 30, 10, 30)
        last_ran = now - timedelta(seconds=61)
        next_minute = 60 - now.second - 1e-6 * now.microsecond
        with patch_crontab_nowfun(self.every_minute, now):
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_execution_is_due_on_monday(self):
        # 31st of May 2010 is a monday
        now = datetime_utc(2010, 5, 31, 10, 30)
        last_ran = now - timedelta(seconds=61)
        next_minute = 60 - now.second - 1e-6 * now.microsecond
        with patch_crontab_nowfun(self.every_minute, now):
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, next_minute, 1)

    def test_every_hour_execution_is_due(self):
        with patch_crontab_nowfun(self.hourly, datetime_utc(2010, 5, 10, 10, 30)):
            due, remaining = self.hourly.is_due(datetime_utc(2010, 5, 10, 6, 30))
            assert due
            assert remaining == 60 * 60

    def test_every_hour_execution_is_not_due(self):
        with patch_crontab_nowfun(self.hourly, datetime_utc(2010, 5, 10, 10, 29)):
            due, remaining = self.hourly.is_due(datetime_utc(2010, 5, 10, 9, 30))
            assert not due
            assert remaining == 60

    def test_first_quarter_execution_is_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime_utc(2010, 5, 10, 10, 15)):
            due, remaining = self.quarterly.is_due(
                datetime_utc(2010, 5, 10, 6, 30),
            )
            assert due
            assert remaining == 15 * 60

    def test_second_quarter_execution_is_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime_utc(2010, 5, 10, 10, 30)):
            due, remaining = self.quarterly.is_due(
                datetime_utc(2010, 5, 10, 6, 30),
            )
            assert due
            assert remaining == 15 * 60

    def test_first_quarter_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime_utc(2010, 5, 10, 10, 14)):
            due, remaining = self.quarterly.is_due(
                datetime_utc(2010, 5, 10, 10, 0),
            )
            assert not due
            assert remaining == 60

    def test_second_quarter_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime_utc(2010, 5, 10, 10, 29)):
            due, remaining = self.quarterly.is_due(
                datetime_utc(2010, 5, 10, 10, 15),
            )
            assert not due
            assert remaining == 60

    def test_daily_execution_is_due(self):
        with patch_crontab_nowfun(self.daily, datetime_utc(2010, 5, 10, 7, 30)):
            due, remaining = self.daily.is_due(datetime_utc(2010, 5, 9, 7, 30))
            assert due
            assert remaining == 24 * 60 * 60

    def test_daily_execution_is_not_due(self):
        with patch_crontab_nowfun(self.daily, datetime_utc(2010, 5, 10, 10, 30)):
            due, remaining = self.daily.is_due(datetime_utc(2010, 5, 10, 7, 30))
            assert not due
            assert remaining == 21 * 60 * 60

    def test_weekly_execution_is_due(self):
        with patch_crontab_nowfun(self.weekly, datetime_utc(2010, 5, 6, 7, 30)):
            due, remaining = self.weekly.is_due(datetime_utc(2010, 4, 30, 7, 30))
            assert due
            assert remaining == 7 * 24 * 60 * 60

    def test_weekly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.weekly, datetime_utc(2010, 5, 7, 10, 30)):
            due, remaining = self.weekly.is_due(datetime_utc(2010, 5, 6, 7, 30))
            assert not due
            assert remaining == 6 * 24 * 60 * 60 - 3 * 60 * 60

    def test_monthly_execution_is_due(self):
        with patch_crontab_nowfun(self.monthly, datetime_utc(2010, 5, 13, 7, 30)):
            due, remaining = self.monthly.is_due(datetime_utc(2010, 4, 8, 7, 30))
            assert due
            assert remaining == 28 * 24 * 60 * 60

    def test_monthly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.monthly, datetime_utc(2010, 5, 9, 10, 30)):
            due, remaining = self.monthly.is_due(datetime_utc(2010, 4, 8, 7, 30))
            assert not due
            assert remaining == 4 * 24 * 60 * 60 - 3 * 60 * 60

    def test_monthly_moy_execution_is_due(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime_utc(2014, 2, 26, 22, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime_utc(2013, 7, 4, 10, 0),
            )
            assert due
            assert remaining == 60.0

    @skip.todo('unstable test')
    def test_monthly_moy_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime_utc(2013, 6, 28, 14, 30)):
            due, remaining = self.monthly_moy.is_due(
                datetime_utc(2013, 6, 28, 22, 14),
            )
            assert not due
            attempt = (
                time.mktime(datetime_utc(2014, 2, 26, 22, 0).timetuple()) -
                time.mktime(datetime_utc(2013, 6, 28, 14, 30).timetuple()) -
                60 * 60
            )
            assert remaining == attempt

    def test_monthly_moy_execution_is_due2(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime_utc(2014, 2, 26, 22, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime_utc(2013, 2, 28, 10, 0),
            )
            assert due
            assert remaining == 60.0

    def test_monthly_moy_execution_is_not_due2(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime_utc(2014, 2, 26, 21, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime_utc(2013, 6, 28, 22, 14),
            )
            assert not due
            attempt = 60 * 60
            assert remaining == attempt

    def test_yearly_execution_is_due(self):
        with patch_crontab_nowfun(self.yearly, datetime_utc(2010, 3, 11, 7, 30)):
            due, remaining = self.yearly.is_due(datetime_utc(2009, 3, 12, 7, 30))
            assert due
            assert remaining == 364 * 24 * 60 * 60

    def test_yearly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.yearly, datetime_utc(2010, 3, 7, 10, 30)):
            due, remaining = self.yearly.is_due(datetime_utc(2009, 3, 12, 7, 30))
            assert not due
            assert remaining == 4 * 24 * 60 * 60 - 3 * 60 * 60

    def test_daily_execution_if_last_run_at_was_days_ago_and_current_time_does_not_match_crontab_schedule_then_execution_is_not_due(self):
        z = self.pytzcrontab(hour=7, minute=30)
        last_run_at = datetime_utc(2018, 6, 1, 7, 30)
        now = datetime_utc(2018, 6, 9, 23, 48)
        expected_next_execution_time = datetime_utc(2018, 6, 10, 7, 30)
        expected_remaining = (expected_next_execution_time - now).total_seconds()
        # check our assumptions
        assert is_time_feasible_wrt_crontab_schedule(last_run_at, z)
        assert not is_time_feasible_wrt_crontab_schedule(now, z)
        assert is_time_feasible_wrt_crontab_schedule(expected_next_execution_time, z)
        assert now < expected_next_execution_time
        assert expected_remaining == (7 * 60 + 30 + 12) * 60
        # test is_due
        with patch_crontab_nowfun(z, now):
            due, remaining = z.is_due(last_run_at=last_run_at)
            assert remaining == expected_remaining
            assert not due

    def test_daily_execution_if_last_run_at_was_the_most_recent_feasible_time_wrt_schedule_in_past_and_current_time_does_not_match_crontab_schedule_then_execution_is_not_due(self):
        z = self.pytzcrontab(hour=7, minute=30)
        last_run_at = datetime_utc(2018, 6, 9, 7, 30)
        now = datetime_utc(2018, 6, 9, 23, 48)
        expected_next_execution_time = datetime_utc(2018, 6, 10, 7, 30)
        expected_remaining = (expected_next_execution_time - now).total_seconds()
        # check our assumptions
        assert is_time_feasible_wrt_crontab_schedule(last_run_at, z)
        assert not is_time_feasible_wrt_crontab_schedule(now, z)
        assert is_time_feasible_wrt_crontab_schedule(expected_next_execution_time, z)
        assert now < expected_next_execution_time
        assert expected_remaining == (7 * 60 + 30 + 12) * 60
        # test is_due
        with patch_crontab_nowfun(z, now):
            due, remaining = z.is_due(last_run_at=last_run_at)
            assert remaining == expected_remaining
            assert not due

    def test_daily_execution_if_last_run_at_was_more_recent_than_the_most_recent_feasible_time_wrt_schedule_in_past_and_current_time_does_not_match_crontab_schedule_then_execution_is_not_due(self):
        z = self.pytzcrontab(hour=7, minute=30)
        last_run_at = datetime_utc(2018, 6, 9, 10, 30) # not feasible wrt to current schedule. case can happen if schedule is modified after a run
        now = datetime_utc(2018, 6, 9, 23, 48)
        expected_next_execution_time = datetime_utc(2018, 6, 10, 7, 30)
        expected_remaining = (expected_next_execution_time - now).total_seconds()
        # check our assumptions
        assert not is_time_feasible_wrt_crontab_schedule(last_run_at, z)
        assert not is_time_feasible_wrt_crontab_schedule(now, z)
        assert is_time_feasible_wrt_crontab_schedule(expected_next_execution_time, z)
        assert now < expected_next_execution_time
        assert expected_remaining == (7 * 60 + 30 + 12) * 60
        # test is_due
        with patch_crontab_nowfun(z, now):
            due, remaining = z.is_due(last_run_at=last_run_at)
            assert remaining == expected_remaining
            assert not due
