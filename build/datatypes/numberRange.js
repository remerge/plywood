import * as tslib_1 from "tslib";
import { Range } from './range';
function finiteOrNull(n) {
    return (isNaN(n) || isFinite(n)) ? n : null;
}
var check;
var NumberRange = (function (_super) {
    tslib_1.__extends(NumberRange, _super);
    function NumberRange(parameters) {
        var _this = this;
        if (isNaN(parameters.start))
            throw new TypeError('`start` must be a number');
        if (isNaN(parameters.end))
            throw new TypeError('`end` must be a number');
        _this = _super.call(this, parameters.start, parameters.end, parameters.bounds) || this;
        return _this;
    }
    NumberRange.isNumberRange = function (candidate) {
        return candidate instanceof NumberRange;
    };
    NumberRange.numberBucket = function (num, size, offset) {
        var start = Math.floor((num - offset) / size) * size + offset;
        return new NumberRange({
            start: start,
            end: start + size,
            bounds: Range.DEFAULT_BOUNDS
        });
    };
    NumberRange.fromNumber = function (n) {
        return new NumberRange({ start: n, end: n, bounds: '[]' });
    };
    NumberRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable numberRange");
        }
        var start = parameters.start;
        var end = parameters.end;
        return new NumberRange({
            start: start === null ? null : finiteOrNull(Number(start)),
            end: end === null ? null : finiteOrNull(Number(end)),
            bounds: parameters.bounds
        });
    };
    NumberRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    NumberRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    NumberRange.prototype.equals = function (other) {
        return other instanceof NumberRange && this._equalsHelper(other);
    };
    NumberRange.prototype.midpoint = function () {
        return (this.start + this.end) / 2;
    };
    NumberRange.prototype.rebaseOnStart = function (newStart) {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        if (!start)
            return this;
        return new NumberRange({
            start: newStart,
            end: end ? end - start + newStart : end,
            bounds: bounds
        });
    };
    NumberRange.type = 'NUMBER_RANGE';
    return NumberRange;
}(Range));
export { NumberRange };
check = NumberRange;
Range.register(NumberRange);
