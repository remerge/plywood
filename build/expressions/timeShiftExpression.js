import * as tslib_1 from "tslib";
import { Duration, Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';
import { ChainableExpression, Expression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
var TimeShiftExpression = (function (_super) {
    tslib_1.__extends(TimeShiftExpression, _super);
    function TimeShiftExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.duration = parameters.duration;
        _this.step = parameters.step != null ? parameters.step : TimeShiftExpression.DEFAULT_STEP;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeShift");
        _this._checkOperandTypes('TIME');
        if (!(_this.duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        _this.type = 'TIME';
        return _this;
    }
    TimeShiftExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        value.step = parameters.step;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeShiftExpression(value);
    };
    TimeShiftExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        value.step = this.step;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeShiftExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        js.step = this.step;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeShiftExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            this.step === other.step &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeShiftExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString(), this.step.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeShiftExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? this.duration.shift(operandValue, this.getTimezone(), this.step) : null;
    };
    TimeShiftExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeShiftExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeShiftExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeShiftExpression.prototype.changeStep = function (step) {
        if (this.step === step)
            return this;
        var value = this.valueOf();
        value.step = step;
        return new TimeShiftExpression(value);
    };
    TimeShiftExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, duration = _a.duration, step = _a.step, timezone = _a.timezone;
        if (step === 0)
            return operand;
        if (operand instanceof TimeShiftExpression) {
            var x = operand.operand, d = operand.duration, s = operand.step, tz = operand.timezone;
            if (duration.equals(d) && immutableEqual(timezone, tz)) {
                return x.timeShift(d, step + s, tz);
            }
        }
        return this;
    };
    TimeShiftExpression.DEFAULT_STEP = 1;
    TimeShiftExpression.op = "TimeShift";
    return TimeShiftExpression;
}(ChainableExpression));
export { TimeShiftExpression };
Expression.applyMixins(TimeShiftExpression, [HasTimezone]);
Expression.register(TimeShiftExpression);
