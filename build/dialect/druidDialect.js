import * as tslib_1 from "tslib";
import { SQLDialect } from './baseDialect';
var DruidDialect = (function (_super) {
    tslib_1.__extends(DruidDialect, _super);
    function DruidDialect() {
        return _super.call(this) || this;
    }
    DruidDialect.prototype.nullConstant = function () {
        return "''";
    };
    DruidDialect.prototype.dateToSQLDateString = function (date) {
        return date.toISOString()
            .replace('T', ' ')
            .replace('Z', '')
            .replace(/\.000$/, '');
    };
    DruidDialect.prototype.constantGroupBy = function () {
        return "GROUP BY ''";
    };
    DruidDialect.prototype.timeToSQL = function (date) {
        if (!date)
            return this.nullConstant();
        return "TIMESTAMP '" + this.dateToSQLDateString(date) + "'";
    };
    DruidDialect.prototype.concatExpression = function (a, b) {
        return "(" + a + "||" + b + ")";
    };
    DruidDialect.prototype.containsExpression = function (a, b) {
        return "POSITION(" + a + " IN " + b + ")>0";
    };
    DruidDialect.prototype.coalesceExpression = function (a, b) {
        return "CASE WHEN " + a + "='' THEN " + b + " ELSE " + a + " END";
    };
    DruidDialect.prototype.substrExpression = function (a, position, length) {
        return "SUBSTRING(" + a + "," + (position + 1) + "," + length + ")";
    };
    DruidDialect.prototype.isNotDistinctFromExpression = function (a, b) {
        return "(" + a + "=" + b + ")";
    };
    DruidDialect.prototype.castExpression = function (inputType, operand, cast) {
        var castFunction = DruidDialect.CAST_TO_FUNCTION[cast][inputType];
        if (!castFunction)
            throw new Error("unsupported cast from " + inputType + " to " + cast + " in Druid dialect");
        return castFunction.replace(/\$\$/g, operand);
    };
    DruidDialect.prototype.timeFloorExpression = function (operand, duration, timezone) {
        var bucketFormat = DruidDialect.TIME_BUCKETING[duration.toString()];
        if (!bucketFormat)
            throw new Error("unsupported duration '" + duration + "'");
        return "FLOOR(" + operand + " TO " + bucketFormat + ")";
    };
    DruidDialect.prototype.timeBucketExpression = function (operand, duration, timezone) {
        return this.timeFloorExpression(operand, duration, timezone);
    };
    DruidDialect.prototype.timePartExpression = function (operand, part, timezone) {
        var timePartFunction = DruidDialect.TIME_PART_TO_FUNCTION[part];
        if (!timePartFunction)
            throw new Error("unsupported part " + part + " in Druid dialect");
        return timePartFunction.replace(/\$\$/g, operand);
    };
    DruidDialect.prototype.timeShiftExpression = function (operand, duration, timezone) {
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
    DruidDialect.prototype.extractExpression = function (operand, regexp) {
        return "(SELECT (REGEXP_MATCHES(" + operand + ", '" + regexp + "'))[1])";
    };
    DruidDialect.prototype.indexOfExpression = function (str, substr) {
        return "POSITION(" + substr + " IN " + str + ") - 1";
    };
    DruidDialect.TIME_BUCKETING = {
        "PT1S": "second",
        "PT1M": "minute",
        "PT1H": "hour",
        "P1D": "day",
        "P1W": "week",
        "P1M": "month",
        "P3M": "quarter",
        "P1Y": "year"
    };
    DruidDialect.TIME_PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: "EXTRACT(SECOND FROM $$)",
        SECOND_OF_HOUR: "(EXTRACT(MINUTE FROM $$)*60+EXTRACT(SECOND FROM $$))",
        SECOND_OF_DAY: "((EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
        SECOND_OF_WEEK: "(((MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
        SECOND_OF_MONTH: "((((EXTRACT(DAY FROM $$)-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
        SECOND_OF_YEAR: "((((TIME_EXTRACT($$,'DOY')-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$))*60+EXTRACT(SECOND FROM $$))",
        MINUTE_OF_HOUR: "EXTRACT(MINUTE FROM $$)",
        MINUTE_OF_DAY: "EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
        MINUTE_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
        MINUTE_OF_MONTH: "((EXTRACT(DAY FROM $$)-1)*24)+EplyXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
        MINUTE_OF_YEAR: "((TIME_EXTRACT($$,'DOY')-1)*24)+EXTRACT(HOUR FROM $$)*60+EXTRACT(MINUTE FROM $$)",
        HOUR_OF_DAY: "EXTRACT(HOUR FROM $$)",
        HOUR_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)*24+EXTRACT(HOUR FROM $$))",
        HOUR_OF_MONTH: "((EXTRACT(DAY FROM $$)-1)*24+EXTRACT(HOUR FROM $$))",
        HOUR_OF_YEAR: "((TIME_EXTRACT($$,'DOY')-1)*24+EXTRACT(HOUR FROM $$))",
        DAY_OF_WEEK: "MOD(CAST((TIME_EXTRACT($$,'DOW')+6) AS int),7)+1",
        DAY_OF_MONTH: "EXTRACT(DAY FROM $$)",
        DAY_OF_YEAR: "TIME_EXTRACT($$,'DOY')",
        WEEK_OF_YEAR: "TIME_EXTRACT($$,'WEEK')",
        MONTH_OF_YEAR: "TIME_EXTRACT($$,'MONTH')",
        YEAR: "EXTRACT(YEAR FROM $$)"
    };
    DruidDialect.CAST_TO_FUNCTION = {
        TIME: {
            NUMBER: 'TO_TIMESTAMP($$::double precision / 1000)'
        },
        NUMBER: {
            TIME: "CAST($$ AS BIGINT)",
            STRING: "CAST($$ AS FLOAT)"
        },
        STRING: {
            NUMBER: "CAST($$ AS VARCHAR)"
        }
    };
    return DruidDialect;
}(SQLDialect));
export { DruidDialect };
