import * as tslib_1 from "tslib";
import { Range } from './range';
var check;
var StringRange = (function (_super) {
    tslib_1.__extends(StringRange, _super);
    function StringRange(parameters) {
        var _this = this;
        var start = parameters.start, end = parameters.end;
        if (typeof start !== 'string' && start !== null)
            throw new TypeError('`start` must be a string');
        if (typeof end !== 'string' && end !== null)
            throw new TypeError('`end` must be a string');
        _this = _super.call(this, start, end, parameters.bounds) || this;
        return _this;
    }
    StringRange.isStringRange = function (candidate) {
        return candidate instanceof StringRange;
    };
    StringRange.fromString = function (s) {
        return new StringRange({ start: s, end: s, bounds: '[]' });
    };
    StringRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable StringRange");
        }
        var start = parameters.start;
        var end = parameters.end;
        var bounds = parameters.bounds;
        return new StringRange({
            start: start, end: end, bounds: bounds
        });
    };
    StringRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    StringRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    StringRange.prototype.equals = function (other) {
        return other instanceof StringRange && this._equalsHelper(other);
    };
    StringRange.prototype.midpoint = function () {
        throw new Error("midpoint not supported in string range");
    };
    StringRange.prototype._zeroEndpoint = function () {
        return "";
    };
    StringRange.prototype.validMemberType = function (val) {
        return typeof val === 'string';
    };
    StringRange.type = 'STRING_RANGE';
    return StringRange;
}(Range));
export { StringRange };
check = StringRange;
Range.register(StringRange);
