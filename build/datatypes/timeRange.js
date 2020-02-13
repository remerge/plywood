import * as tslib_1 from "tslib";
import { parseISODate, Timezone } from 'chronoshift';
import { Expression } from '../expressions/baseExpression';
import { NumberRange } from './numberRange';
import { Range } from './range';
function toDate(date, name) {
    if (date === null)
        return null;
    var typeofDate = typeof date;
    if (typeofDate === "undefined")
        throw new TypeError("timeRange must have a " + name);
    if (typeofDate === 'string') {
        var parsedDate = parseISODate(date, Expression.defaultParserTimezone);
        if (!parsedDate)
            throw new Error("could not parse '" + date + "' as date");
        date = parsedDate;
    }
    else if (typeofDate === 'number') {
        date = new Date(date);
    }
    if (!date.getDay)
        throw new TypeError("timeRange must have a " + name + " that is a Date");
    return date;
}
var START_OF_TIME = "1000";
var END_OF_TIME = "3000";
function dateToIntervalPart(date) {
    return date.toISOString()
        .replace('.000Z', 'Z')
        .replace(':00Z', 'Z')
        .replace(':00Z', 'Z');
}
var check;
var TimeRange = (function (_super) {
    tslib_1.__extends(TimeRange, _super);
    function TimeRange(parameters) {
        return _super.call(this, parameters.start, parameters.end, parameters.bounds) || this;
    }
    TimeRange.isTimeRange = function (candidate) {
        return candidate instanceof TimeRange;
    };
    TimeRange.intervalFromDate = function (date) {
        return dateToIntervalPart(date) + '/' + dateToIntervalPart(new Date(date.valueOf() + 1));
    };
    TimeRange.timeBucket = function (date, duration, timezone) {
        if (!date)
            return null;
        var start = duration.floor(date, timezone);
        return new TimeRange({
            start: start,
            end: duration.shift(start, timezone, 1),
            bounds: Range.DEFAULT_BOUNDS
        });
    };
    TimeRange.fromTime = function (t) {
        return new TimeRange({ start: t, end: t, bounds: '[]' });
    };
    TimeRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable timeRange");
        }
        return new TimeRange({
            start: toDate(parameters.start, 'start'),
            end: toDate(parameters.end, 'end'),
            bounds: parameters.bounds
        });
    };
    TimeRange.prototype._zeroEndpoint = function () {
        return new Date(0);
    };
    TimeRange.prototype._endpointEqual = function (a, b) {
        if (a === null) {
            return b === null;
        }
        else {
            return b !== null && a.valueOf() === b.valueOf();
        }
    };
    TimeRange.prototype._endpointToString = function (a, tz) {
        return a ? Timezone.formatDateWithTimezone(a, tz) : 'null';
    };
    TimeRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    TimeRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    TimeRange.prototype.equals = function (other) {
        return other instanceof TimeRange && this._equalsHelper(other);
    };
    TimeRange.prototype.toInterval = function () {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        var interval = [START_OF_TIME, END_OF_TIME];
        if (start) {
            if (bounds[0] === '(')
                start = new Date(start.valueOf() + 1);
            interval[0] = dateToIntervalPart(start);
        }
        if (end) {
            if (bounds[1] === ']')
                end = new Date(end.valueOf() + 1);
            interval[1] = dateToIntervalPart(end);
        }
        return interval.join("/");
    };
    TimeRange.prototype.midpoint = function () {
        return new Date((this.start.valueOf() + this.end.valueOf()) / 2);
    };
    TimeRange.prototype.changeToNumber = function () {
        return new NumberRange({
            bounds: this.bounds,
            start: this.start ? this.start.valueOf() : null,
            end: this.end ? this.end.valueOf() : null
        });
    };
    TimeRange.prototype.isAligned = function (duration, timezone) {
        var _a = this, start = _a.start, end = _a.end;
        return (!start || duration.isAligned(start, timezone)) && (!end || duration.isAligned(end, timezone));
    };
    TimeRange.prototype.rebaseOnStart = function (newStart) {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        if (!start)
            return this;
        return new TimeRange({
            start: newStart,
            end: end ? new Date(end.valueOf() - start.valueOf() + newStart.valueOf()) : end,
            bounds: bounds
        });
    };
    TimeRange.type = 'TIME_RANGE';
    return TimeRange;
}(Range));
export { TimeRange };
check = TimeRange;
Range.register(TimeRange);
