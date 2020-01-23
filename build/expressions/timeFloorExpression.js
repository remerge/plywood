import * as tslib_1 from "tslib";
import { Duration, Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';
import { Set, TimeRange } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
import { OverlapExpression } from './overlapExpression';
import { TimeBucketExpression } from './timeBucketExpression';
var TimeFloorExpression = (function (_super) {
    tslib_1.__extends(TimeFloorExpression, _super);
    function TimeFloorExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var duration = parameters.duration;
        _this.duration = duration;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeFloor");
        _this._bumpOperandToTime();
        _this._checkOperandTypes('TIME');
        if (!(duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        if (!duration.isFloorable()) {
            throw new Error("duration '" + duration.toString() + "' is not floorable");
        }
        _this.type = 'TIME';
        return _this;
    }
    TimeFloorExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeFloorExpression(value);
    };
    TimeFloorExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeFloorExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeFloorExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeFloorExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeFloorExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? this.duration.floor(operandValue, this.getTimezone()) : null;
    };
    TimeFloorExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeFloorExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeFloorExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeFloorExpression.prototype.alignsWith = function (ex) {
        var _a = this, timezone = _a.timezone, duration = _a.duration;
        if (!timezone)
            return false;
        if (ex instanceof TimeFloorExpression || ex instanceof TimeBucketExpression) {
            return timezone.equals(ex.timezone) && ex.duration.dividesBy(duration);
        }
        if (ex instanceof OverlapExpression) {
            var literal = ex.expression.getLiteralValue();
            if (literal instanceof TimeRange) {
                return literal.isAligned(duration, timezone);
            }
            else if (literal instanceof Set) {
                if (literal.setType !== 'TIME_RANGE')
                    return false;
                return literal.elements.every(function (e) {
                    return e.isAligned(duration, timezone);
                });
            }
        }
        return false;
    };
    TimeFloorExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, duration = _a.duration, timezone = _a.timezone;
        if (operand instanceof TimeFloorExpression) {
            var d = operand.duration, tz = operand.timezone;
            if (duration.equals(d) && immutableEqual(timezone, tz))
                return operand;
        }
        return this;
    };
    TimeFloorExpression.op = "TimeFloor";
    return TimeFloorExpression;
}(ChainableExpression));
export { TimeFloorExpression };
Expression.applyMixins(TimeFloorExpression, [HasTimezone]);
Expression.register(TimeFloorExpression);
