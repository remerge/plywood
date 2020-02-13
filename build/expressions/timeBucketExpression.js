import * as tslib_1 from "tslib";
import { Duration, Timezone } from 'chronoshift';
import { immutableEqual } from 'immutable-class';
import { TimeRange } from '../datatypes/timeRange';
import { ChainableExpression, Expression } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
var TimeBucketExpression = (function (_super) {
    tslib_1.__extends(TimeBucketExpression, _super);
    function TimeBucketExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var duration = parameters.duration;
        _this.duration = duration;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeBucket");
        _this._checkOperandTypes('TIME');
        if (!(duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        if (!duration.isFloorable()) {
            throw new Error("duration '" + duration.toString() + "' is not floorable");
        }
        _this.type = 'TIME_RANGE';
        return _this;
    }
    TimeBucketExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeBucketExpression(value);
    };
    TimeBucketExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeBucketExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeBucketExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeBucketExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeBucketExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? TimeRange.timeBucket(operandValue, this.duration, this.getTimezone()) : null;
    };
    TimeBucketExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeBucketExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeBucketExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeBucketExpression.op = "TimeBucket";
    return TimeBucketExpression;
}(ChainableExpression));
export { TimeBucketExpression };
Expression.applyMixins(TimeBucketExpression, [HasTimezone]);
Expression.register(TimeBucketExpression);
