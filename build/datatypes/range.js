var BOUNDS_REG_EXP = /^[\[(][\])]$/;
var Range = (function () {
    function Range(start, end, bounds) {
        if (bounds) {
            if (!BOUNDS_REG_EXP.test(bounds)) {
                throw new Error("invalid bounds " + bounds);
            }
        }
        else {
            bounds = Range.DEFAULT_BOUNDS;
        }
        if (start !== null && end !== null && this._endpointEqual(start, end)) {
            if (bounds !== '[]') {
                start = end = this._zeroEndpoint();
            }
            if (bounds === '(]' || bounds === '()')
                this.bounds = '[)';
        }
        else {
            if (start !== null && end !== null && end < start) {
                throw new Error('must have start <= end');
            }
            if (start === null && bounds[0] === '[') {
                bounds = '(' + bounds[1];
            }
            if (end === null && bounds[1] === ']') {
                bounds = bounds[0] + ')';
            }
        }
        this.start = start;
        this.end = end;
        this.bounds = bounds;
    }
    Range.isRange = function (candidate) {
        return candidate instanceof Range;
    };
    Range.isRangeType = function (type) {
        return type && type.indexOf('_RANGE') > 0;
    };
    Range.unwrapRangeType = function (type) {
        if (!type)
            return null;
        return Range.isRangeType(type) ? type.substr(0, type.length - 6) : type;
    };
    Range.register = function (ctr) {
        var rangeType = ctr.type.replace('_RANGE', '').toLowerCase();
        Range.classMap[rangeType] = ctr;
    };
    Range.fromJS = function (parameters) {
        var ctr;
        if (typeof parameters.start === 'number' || typeof parameters.end === 'number') {
            ctr = 'number';
        }
        else if (typeof parameters.start === 'string' || typeof parameters.end === 'string') {
            ctr = 'string';
        }
        else {
            ctr = 'time';
        }
        return Range.classMap[ctr].fromJS(parameters);
    };
    Range.prototype._zeroEndpoint = function () {
        return 0;
    };
    Range.prototype._endpointEqual = function (a, b) {
        return a === b;
    };
    Range.prototype._endpointToString = function (a, tz) {
        return String(a);
    };
    Range.prototype._equalsHelper = function (other) {
        return Boolean(other) &&
            this.bounds === other.bounds &&
            this._endpointEqual(this.start, other.start) &&
            this._endpointEqual(this.end, other.end);
    };
    Range.prototype.toJSON = function () {
        return this.toJS();
    };
    Range.prototype.toString = function (tz) {
        var bounds = this.bounds;
        return '[' + (bounds[0] === '(' ? '~' : '') + this._endpointToString(this.start, tz) + ',' + this._endpointToString(this.end, tz) + (bounds[1] === ')' ? '' : '!') + ']';
    };
    Range.prototype.compare = function (other) {
        var myStart = this.start;
        var otherStart = other.start;
        return myStart < otherStart ? -1 : (otherStart < myStart ? 1 : 0);
    };
    Range.prototype.openStart = function () {
        return this.bounds[0] === '(';
    };
    Range.prototype.openEnd = function () {
        return this.bounds[1] === ')';
    };
    Range.prototype.empty = function () {
        return this._endpointEqual(this.start, this.end) && this.bounds === '[)';
    };
    Range.prototype.degenerate = function () {
        return this._endpointEqual(this.start, this.end) && this.bounds === '[]';
    };
    Range.prototype.contains = function (val) {
        if (val instanceof Range) {
            var valStart = val.start;
            var valEnd = val.end;
            var valBound = val.bounds;
            if (valBound[0] === '[') {
                if (!this.containsValue(valStart))
                    return false;
            }
            else {
                if (!this.containsValue(valStart) && valStart.valueOf() !== this.start.valueOf())
                    return false;
            }
            if (valBound[1] === ']') {
                if (!this.containsValue(valEnd))
                    return false;
            }
            else {
                if (!this.containsValue(valEnd) && valEnd.valueOf() !== this.end.valueOf())
                    return false;
            }
            return true;
        }
        else {
            return this.containsValue(val);
        }
    };
    Range.prototype.validMemberType = function (val) {
        return typeof val === 'number';
    };
    Range.prototype.containsValue = function (val) {
        if (val === null)
            return false;
        val = val.valueOf();
        if (!this.validMemberType(val))
            return false;
        var start = this.start;
        var end = this.end;
        var bounds = this.bounds;
        if (bounds[0] === '[') {
            if (val < start)
                return false;
        }
        else {
            if (start !== null && val <= start)
                return false;
        }
        if (bounds[1] === ']') {
            if (end < val)
                return false;
        }
        else {
            if (end !== null && end <= val)
                return false;
        }
        return true;
    };
    Range.prototype.intersects = function (other) {
        return this.containsValue(other.start)
            || this.containsValue(other.end)
            || other.containsValue(this.start)
            || other.containsValue(this.end)
            || this._equalsHelper(other);
    };
    Range.prototype.adjacent = function (other) {
        return (this._endpointEqual(this.end, other.start) && this.openEnd() !== other.openStart())
            || (this._endpointEqual(this.start, other.end) && this.openStart() !== other.openEnd());
    };
    Range.prototype.mergeable = function (other) {
        return this.intersects(other) || this.adjacent(other);
    };
    Range.prototype.union = function (other) {
        if (!this.mergeable(other))
            return null;
        return this.extend(other);
    };
    Range.prototype.extent = function () {
        return this;
    };
    Range.prototype.extend = function (other) {
        var thisStart = this.start;
        var thisEnd = this.end;
        var otherStart = other.start;
        var otherEnd = other.end;
        var start;
        var startBound;
        if (thisStart === null || otherStart === null) {
            start = null;
            startBound = '(';
        }
        else if (thisStart < otherStart) {
            start = thisStart;
            startBound = this.bounds[0];
        }
        else {
            start = otherStart;
            startBound = other.bounds[0];
        }
        var end;
        var endBound;
        if (thisEnd === null || otherEnd === null) {
            end = null;
            endBound = ')';
        }
        else if (thisEnd < otherEnd) {
            end = otherEnd;
            endBound = other.bounds[1];
        }
        else {
            end = thisEnd;
            endBound = this.bounds[1];
        }
        return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
    };
    Range.prototype.intersect = function (other) {
        if (!this.mergeable(other))
            return null;
        var thisStart = this.start;
        var thisEnd = this.end;
        var otherStart = other.start;
        var otherEnd = other.end;
        var start;
        var startBound;
        if (thisStart === null || otherStart === null) {
            if (otherStart === null) {
                start = thisStart;
                startBound = this.bounds[0];
            }
            else {
                start = otherStart;
                startBound = other.bounds[0];
            }
        }
        else if (otherStart < thisStart) {
            start = thisStart;
            startBound = this.bounds[0];
        }
        else {
            start = otherStart;
            startBound = other.bounds[0];
        }
        var end;
        var endBound;
        if (thisEnd === null || otherEnd === null) {
            if (thisEnd == null) {
                end = otherEnd;
                endBound = other.bounds[1];
            }
            else {
                end = thisEnd;
                endBound = this.bounds[1];
            }
        }
        else if (otherEnd < thisEnd) {
            end = otherEnd;
            endBound = other.bounds[1];
        }
        else {
            end = thisEnd;
            endBound = this.bounds[1];
        }
        return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
    };
    Range.prototype.isFinite = function () {
        return this.start !== null && this.end !== null;
    };
    Range.DEFAULT_BOUNDS = '[)';
    Range.classMap = {};
    return Range;
}());
export { Range };
