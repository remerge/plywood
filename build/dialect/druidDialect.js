import * as tslib_1 from "tslib";
import { SQLDialect } from './baseDialect';
var DruidDialect = (function (_super) {
    tslib_1.__extends(DruidDialect, _super);
    function DruidDialect() {
        return _super.call(this) || this;
    }
    DruidDialect.prototype.dateToSQLDateString = function (date) {
        return date.toISOString()
            .replace('T', ' ')
            .replace('Z', '')
            .replace(/\.000$/, '');
    };
    DruidDialect.prototype.floatDivision = function (numerator, denominator) {
        return "(" + numerator + "*1.0/" + denominator + ")";
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
    DruidDialect.prototype.substrExpression = function (a, position, length) {
        return "SUBSTRING(" + a + "," + (position + 1) + "," + length + ")";
    };
    DruidDialect.prototype.isNotDistinctFromExpression = function (a, b) {
        var nullConst = this.nullConstant();
        if (a === nullConst)
            return b + " IS " + nullConst;
        if (b === nullConst)
            return a + " IS " + nullConst;
        return "(" + a + "=" + b + ")";
    };
    DruidDialect.prototype.castExpression = function (inputType, operand, cast) {
        var castFunction = DruidDialect.CAST_TO_FUNCTION[cast][inputType];
        if (!castFunction)
            throw new Error("unsupported cast from " + inputType + " to " + cast + " in Druid dialect");
        return castFunction.replace(/\$\$/g, operand);
    };
    DruidDialect.prototype.operandAsTimestamp = function (operand) {
        return operand.includes('__time') ? operand : "TIME_PARSE(" + operand + ")";
    };
    DruidDialect.prototype.timeFloorExpression = function (operand, duration, timezone) {
        return "TIME_FLOOR(" + this.operandAsTimestamp(operand) + ", " + this.escapeLiteral(duration.toString()) + ", NULL, " + this.escapeLiteral(timezone.toString()) + ")";
    };
    DruidDialect.prototype.timeBucketExpression = function (operand, duration, timezone) {
        return this.timeFloorExpression(operand, duration, timezone);
    };
    DruidDialect.prototype.timePartExpression = function (operand, part, timezone) {
        var timePartFunction = DruidDialect.TIME_PART_TO_FUNCTION[part];
        if (!timePartFunction)
            throw new Error("unsupported part " + part + " in Druid dialect");
        return timePartFunction
            .replace(/\$\$/g, this.operandAsTimestamp(operand))
            .replace(/##/g, this.escapeLiteral(timezone.toString()));
    };
    DruidDialect.prototype.timeShiftExpression = function (operand, duration, step, timezone) {
        return "TIME_SHIFT(" + this.operandAsTimestamp(operand) + ", " + this.escapeLiteral(duration.toString()) + ", " + step + ", " + this.escapeLiteral(timezone.toString()) + ")";
    };
    DruidDialect.prototype.extractExpression = function (operand, regexp) {
        return "REGEXP_EXTRACT(" + operand + ", " + this.escapeLiteral(regexp) + ", 1)";
    };
    DruidDialect.prototype.regexpExpression = function (expression, regexp) {
        return "REGEXP_LIKE(" + expression + ", " + this.escapeLiteral(regexp) + ")";
    };
    DruidDialect.prototype.indexOfExpression = function (str, substr) {
        return "POSITION(" + substr + " IN " + str + ") - 1";
    };
    DruidDialect.prototype.logExpression = function (base, operand) {
        if (base === String(Math.E))
            return "LN(" + operand + ")";
        if (base === '10')
            return "LOG10(" + operand + ")";
        return "LN(" + operand + ")/LN(" + base + ")";
    };
    DruidDialect.TIME_PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: "TIME_EXTRACT($$,'SECOND',##)",
        SECOND_OF_HOUR: "(TIME_EXTRACT($$,'MINUTE',##)*60+TIME_EXTRACT($$,'SECOND',##))",
        SECOND_OF_DAY: "((TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
        SECOND_OF_WEEK: "(((MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
        SECOND_OF_MONTH: "((((TIME_EXTRACT($$,'DAY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
        SECOND_OF_YEAR: "((((TIME_EXTRACT($$,'DOY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##))*60+TIME_EXTRACT($$,'SECOND',##))",
        MINUTE_OF_HOUR: "TIME_EXTRACT($$,'MINUTE',##)",
        MINUTE_OF_DAY: "TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
        MINUTE_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
        MINUTE_OF_MONTH: "((TIME_EXTRACT($$,'DAY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
        MINUTE_OF_YEAR: "((TIME_EXTRACT($$,'DOY',##)-1)*24)+TIME_EXTRACT($$,'HOUR',##)*60+TIME_EXTRACT($$,'MINUTE',##)",
        HOUR_OF_DAY: "TIME_EXTRACT($$,'HOUR',##)",
        HOUR_OF_WEEK: "(MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)*24+TIME_EXTRACT($$,'HOUR',##))",
        HOUR_OF_MONTH: "((TIME_EXTRACT($$,'DAY',##)-1)*24+TIME_EXTRACT($$,'HOUR',##))",
        HOUR_OF_YEAR: "((TIME_EXTRACT($$,'DOY',##)-1)*24+TIME_EXTRACT($$,'HOUR',##))",
        DAY_OF_WEEK: "MOD(CAST((TIME_EXTRACT($$,'DOW',##)+6) AS int),7)+1",
        DAY_OF_MONTH: "TIME_EXTRACT($$,'DAY',##)",
        DAY_OF_YEAR: "TIME_EXTRACT($$,'DOY',##)",
        WEEK_OF_YEAR: "TIME_EXTRACT($$,'WEEK',##)",
        MONTH_OF_YEAR: "TIME_EXTRACT($$,'MONTH',##)",
        YEAR: "TIME_EXTRACT($$,'YEAR',##)"
    };
    DruidDialect.CAST_TO_FUNCTION = {
        TIME: {
            NUMBER: 'MILLIS_TO_TIMESTAMP(CAST($$ AS BIGINT))'
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
