import * as tslib_1 from "tslib";
import { Duration, Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';
import { TimeRange } from '../datatypes/timeRange';
import { pluralIfNeeded } from '../helper/utils';
import { ChainableExpression, Expression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
var TimeRangeExpression = (function (_super) {
    tslib_1.__extends(TimeRangeExpression, _super);
    function TimeRangeExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.duration = parameters.duration;
        _this.step = parameters.step || TimeRangeExpression.DEFAULT_STEP;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeRange");
        _this._checkOperandTypes('TIME');
        if (!(_this.duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        _this.type = 'TIME_RANGE';
        return _this;
    }
    TimeRangeExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        value.step = parameters.step;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeRangeExpression(value);
    };
    TimeRangeExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        value.step = this.step;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeRangeExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        js.step = this.step;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeRangeExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            this.step === other.step &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeRangeExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString(), this.step.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeRangeExpression.prototype.getQualifiedDurationDescription = function (capitalize) {
        var step = Math.abs(this.step);
        var durationDescription = this.duration.getDescription(capitalize);
        return step !== 1 ? pluralIfNeeded(step, durationDescription) : durationDescription;
    };
    TimeRangeExpression.prototype._calcChainableHelper = function (operandValue) {
        var duration = this.duration;
        var step = this.step;
        var timezone = this.getTimezone();
        if (operandValue === null)
            return null;
        var other = duration.shift(operandValue, timezone, step);
        if (step > 0) {
            return new TimeRange({ start: operandValue, end: other });
        }
        else {
            return new TimeRange({ start: other, end: operandValue });
        }
    };
    TimeRangeExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeRangeExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error("implement me");
    };
    TimeRangeExpression.DEFAULT_STEP = 1;
    TimeRangeExpression.op = "TimeRange";
    return TimeRangeExpression;
}(ChainableExpression));
export { TimeRangeExpression };
Expression.applyMixins(TimeRangeExpression, [HasTimezone]);
Expression.register(TimeRangeExpression);
