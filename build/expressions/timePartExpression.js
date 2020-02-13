import * as tslib_1 from "tslib";
import { Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';
import * as moment from 'moment-timezone';
import { ChainableExpression, Expression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
var TimePartExpression = (function (_super) {
    tslib_1.__extends(TimePartExpression, _super);
    function TimePartExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.part = parameters.part;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timePart");
        _this._checkOperandTypes('TIME');
        if (typeof _this.part !== 'string') {
            throw new Error("`part` must be a string");
        }
        _this.type = 'NUMBER';
        return _this;
    }
    TimePartExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.part = parameters.part;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimePartExpression(value);
    };
    TimePartExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.part = this.part;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimePartExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.part = this.part;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimePartExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.part === other.part &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimePartExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.part];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimePartExpression.prototype._calcChainableHelper = function (operandValue) {
        var part = this.part;
        var parter = TimePartExpression.PART_TO_FUNCTION[part];
        if (!parter)
            throw new Error("unsupported part '" + part + "'");
        if (!operandValue)
            return null;
        return parter(moment.tz(operandValue, this.getTimezone().toString()));
    };
    TimePartExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimePartExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timePartExpression(operandSQL, this.part, this.getTimezone());
    };
    TimePartExpression.prototype.maxPossibleSplitValues = function () {
        var maxValue = TimePartExpression.PART_TO_MAX_VALUES[this.part];
        if (!maxValue)
            return Infinity;
        return maxValue + 1;
    };
    TimePartExpression.op = "TimePart";
    TimePartExpression.PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: function (d) { return d.seconds(); },
        SECOND_OF_HOUR: function (d) { return d.minutes() * 60 + d.seconds(); },
        SECOND_OF_DAY: function (d) { return (d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_WEEK: function (d) { return ((d.day() * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_MONTH: function (d) { return (((d.date() - 1) * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_YEAR: function (d) { return (((d.dayOfYear() - 1) * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        MINUTE_OF_HOUR: function (d) { return d.minutes(); },
        MINUTE_OF_DAY: function (d) { return d.hours() * 60 + d.minutes(); },
        MINUTE_OF_WEEK: function (d) { return (d.day() * 24) + d.hours() * 60 + d.minutes(); },
        MINUTE_OF_MONTH: function (d) { return ((d.date() - 1) * 24) + d.hours() * 60 + d.minutes(); },
        MINUTE_OF_YEAR: function (d) { return ((d.dayOfYear() - 1) * 24) + d.hours() * 60 + d.minutes(); },
        HOUR_OF_DAY: function (d) { return d.hours(); },
        HOUR_OF_WEEK: function (d) { return d.day() * 24 + d.hours(); },
        HOUR_OF_MONTH: function (d) { return (d.date() - 1) * 24 + d.hours(); },
        HOUR_OF_YEAR: function (d) { return (d.dayOfYear() - 1) * 24 + d.hours(); },
        DAY_OF_WEEK: function (d) { return d.day() || 7; },
        DAY_OF_MONTH: function (d) { return d.date(); },
        DAY_OF_YEAR: function (d) { return d.dayOfYear(); },
        MONTH_OF_YEAR: function (d) { return d.month(); },
        YEAR: function (d) { return d.year(); },
        QUARTER: function (d) { return d.quarter(); }
    };
    TimePartExpression.PART_TO_MAX_VALUES = {
        SECOND_OF_MINUTE: 61,
        SECOND_OF_HOUR: 3601,
        SECOND_OF_DAY: 93601,
        MINUTE_OF_HOUR: 60,
        MINUTE_OF_DAY: 26 * 60,
        HOUR_OF_DAY: 26,
        DAY_OF_WEEK: 7,
        DAY_OF_MONTH: 31,
        DAY_OF_YEAR: 366,
        WEEK_OF_MONTH: 5,
        WEEK_OF_YEAR: 53,
        MONTH_OF_YEAR: 12
    };
    return TimePartExpression;
}(ChainableExpression));
export { TimePartExpression };
Expression.applyMixins(TimePartExpression, [HasTimezone]);
Expression.register(TimePartExpression);
