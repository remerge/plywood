import * as tslib_1 from "tslib";
import { SQLDialect } from './baseDialect';
var PostgresDialect = (function (_super) {
    tslib_1.__extends(PostgresDialect, _super);
    function PostgresDialect() {
        return _super.call(this) || this;
    }
    PostgresDialect.prototype.constantGroupBy = function () {
        return "GROUP BY ''=''";
    };
    PostgresDialect.prototype.timeToSQL = function (date) {
        if (!date)
            return this.nullConstant();
        return "TIMESTAMP '" + this.dateToSQLDateString(date) + "'";
    };
    PostgresDialect.prototype.concatExpression = function (a, b) {
        return "(" + a + "||" + b + ")";
    };
    PostgresDialect.prototype.containsExpression = function (a, b) {
        return "POSITION(" + a + " IN " + b + ")>0";
    };
    PostgresDialect.prototype.regexpExpression = function (expression, regexp) {
        return "(" + expression + " ~ '" + regexp + "')";
    };
    PostgresDialect.prototype.castExpression = function (inputType, operand, cast) {
        var castFunction = PostgresDialect.CAST_TO_FUNCTION[cast][inputType];
        if (!castFunction)
            throw new Error("unsupported cast from " + inputType + " to " + cast + " in Postgres dialect");
        return castFunction.replace(/\$\$/g, operand);
    };
    PostgresDialect.prototype.utcToWalltime = function (operand, timezone) {
        if (timezone.isUTC())
            return operand;
        return "(" + operand + " AT TIME ZONE 'UTC' AT TIME ZONE '" + timezone + "')";
    };
    PostgresDialect.prototype.walltimeToUTC = function (operand, timezone) {
        if (timezone.isUTC())
            return operand;
        return "(" + operand + " AT TIME ZONE '" + timezone + "' AT TIME ZONE 'UTC')";
    };
    PostgresDialect.prototype.timeFloorExpression = function (operand, duration, timezone) {
        var bucketFormat = PostgresDialect.TIME_BUCKETING[duration.toString()];
        if (!bucketFormat)
            throw new Error("unsupported duration '" + duration + "'");
        return this.walltimeToUTC("DATE_TRUNC('" + bucketFormat + "'," + this.utcToWalltime(operand, timezone) + ")", timezone);
    };
    PostgresDialect.prototype.timeBucketExpression = function (operand, duration, timezone) {
        return this.timeFloorExpression(operand, duration, timezone);
    };
    PostgresDialect.prototype.timePartExpression = function (operand, part, timezone) {
        var timePartFunction = PostgresDialect.TIME_PART_TO_FUNCTION[part];
        if (!timePartFunction)
            throw new Error("unsupported part " + part + " in Postgres dialect");
        return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
    };
    PostgresDialect.prototype.timeShiftExpression = function (operand, duration, timezone) {
        var sqlFn = "DATE_ADD(";
        var spans = duration.valueOf();
        if (spans.week) {
            return sqlFn + operand + ", INTERVAL " + String(spans.week) + ' WEEK)';
        }
        if (spans.year || spans.month) {
            var expr = String(spans.year || 0) + "-" + String(spans.month || 0);
            operand = sqlFn + operand + ", INTERVAL '" + expr + "' YEAR_MONTH)";
        }
        if (spans.day || spans.hour || spans.minute || spans.second) {
            var expr = String(spans.day || 0) + " " + [spans.hour || 0, spans.minute || 0, spans.second || 0].join(':');
            operand = sqlFn + operand + ", INTERVAL '" + expr + "' DAY_SECOND)";
        }
        return operand;
    };
    PostgresDialect.prototype.extractExpression = function (operand, regexp) {
        return "(SELECT (REGEXP_MATCHES(" + operand + ", '" + regexp + "'))[1])";
    };
    PostgresDialect.prototype.indexOfExpression = function (str, substr) {
        return "POSITION(" + substr + " IN " + str + ") - 1";
    };
    PostgresDialect.TIME_BUCKETING = {
        "PT1S": "second",
        "PT1M": "minute",
        "PT1H": "hour",
        "P1D": "day",
        "P1W": "week",
        "P1M": "month",
        "P3M": "quarter",
        "P1Y": "year"
    };
    PostgresDialect.TIME_PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: "DATE_PART('second',$$)",
        SECOND_OF_HOUR: "(DATE_PART('minute',$$)*60+DATE_PART('second',$$))",
        SECOND_OF_DAY: "((DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
        SECOND_OF_WEEK: "((((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
        SECOND_OF_MONTH: "((((DATE_PART('day',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
        SECOND_OF_YEAR: "((((DATE_PART('doy',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
        MINUTE_OF_HOUR: "DATE_PART('minute',$$)",
        MINUTE_OF_DAY: "DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
        MINUTE_OF_WEEK: "((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
        MINUTE_OF_MONTH: "((DATE_PART('day',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
        MINUTE_OF_YEAR: "((DATE_PART('doy',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
        HOUR_OF_DAY: "DATE_PART('hour',$$)",
        HOUR_OF_WEEK: "((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24+DATE_PART('hour',$$))",
        HOUR_OF_MONTH: "((DATE_PART('day',$$)-1)*24+DATE_PART('hour',$$))",
        HOUR_OF_YEAR: "((DATE_PART('doy',$$)-1)*24+DATE_PART('hour',$$))",
        DAY_OF_WEEK: "(CAST((DATE_PART('dow',$$)+6) AS int)%7)+1",
        DAY_OF_MONTH: "DATE_PART('day',$$)",
        DAY_OF_YEAR: "DATE_PART('doy',$$)",
        WEEK_OF_YEAR: "DATE_PART('week',$$)",
        MONTH_OF_YEAR: "DATE_PART('month',$$)",
        YEAR: "DATE_PART('year',$$)"
    };
    PostgresDialect.CAST_TO_FUNCTION = {
        TIME: {
            NUMBER: 'TO_TIMESTAMP($$::double precision / 1000)'
        },
        NUMBER: {
            TIME: "EXTRACT(EPOCH FROM $$) * 1000",
            STRING: "$$::float"
        },
        STRING: {
            NUMBER: "$$::text"
        }
    };
    return PostgresDialect;
}(SQLDialect));
export { PostgresDialect };
