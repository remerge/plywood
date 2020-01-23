import { isDate } from 'chronoshift';
import { NamedArray } from 'immutable-class';
import { NumberRange, Range, Set, TimeRange } from '../../datatypes/index';
import { r, AndExpression, ContainsExpression, Expression, IsExpression, LiteralExpression, MatchExpression, NotExpression, OrExpression, OverlapExpression, RefExpression } from '../../expressions';
import { External } from '../baseExternal';
import { DruidExpressionBuilder } from './druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';
var DruidFilterBuilder = (function () {
    function DruidFilterBuilder(options) {
        this.version = options.version;
        this.rawAttributes = options.rawAttributes;
        this.timeAttribute = options.timeAttribute;
        this.allowEternity = options.allowEternity;
        this.customTransforms = options.customTransforms;
    }
    DruidFilterBuilder.prototype.filterToDruid = function (filter) {
        var _this = this;
        if (!filter.canHaveType('BOOLEAN'))
            throw new Error("can not filter on " + filter.type);
        if (filter.equals(Expression.FALSE)) {
            return {
                intervals: [],
                filter: null
            };
        }
        else {
            var _a = filter.extractFromAnd(function (ex) {
                return (ex instanceof IsExpression || ex instanceof OverlapExpression) && _this.isTimeRef(ex.operand) && ex.expression instanceof LiteralExpression;
            }), extract = _a.extract, rest = _a.rest;
            return {
                intervals: this.timeFilterToIntervals(extract),
                filter: this.timelessFilterToFilter(rest)
            };
        }
    };
    DruidFilterBuilder.prototype.timeFilterToIntervals = function (filter) {
        if (!filter.canHaveType('BOOLEAN'))
            throw new Error("can not filter on " + filter.type);
        if (filter instanceof LiteralExpression) {
            if (!filter.value)
                return [];
            if (!this.allowEternity)
                throw new Error('must filter on time unless the allowEternity flag is set');
            return DruidFilterBuilder.TRUE_INTERVAL;
        }
        else if (filter instanceof IsExpression) {
            var lhs = filter.operand, rhs = filter.expression;
            if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
                return this.valueToIntervals(rhs.value);
            }
            else {
                throw new Error("can not convert " + filter + " to Druid interval");
            }
        }
        else if (filter instanceof OverlapExpression) {
            var lhs = filter.operand, rhs = filter.expression;
            if (lhs instanceof RefExpression && rhs instanceof LiteralExpression) {
                return this.valueToIntervals(rhs.value);
            }
            else {
                throw new Error("can not convert " + filter + " to Druid intervals");
            }
        }
        else {
            throw new Error("can not convert " + filter + " to Druid intervals");
        }
    };
    DruidFilterBuilder.prototype.timelessFilterToFilter = function (filter) {
        var _this = this;
        if (!filter.canHaveType('BOOLEAN'))
            throw new Error("can not filter on " + filter.type);
        if (filter instanceof RefExpression) {
            filter = filter.is(true);
        }
        if (filter instanceof LiteralExpression) {
            if (filter.value === true) {
                return null;
            }
            else {
                throw new Error("should never get here");
            }
        }
        else if (filter instanceof NotExpression) {
            return {
                type: 'not',
                field: this.timelessFilterToFilter(filter.operand)
            };
        }
        else if (filter instanceof AndExpression) {
            return {
                type: 'and',
                fields: filter.getExpressionList().map(function (p) { return _this.timelessFilterToFilter(p); })
            };
        }
        else if (filter instanceof OrExpression) {
            return {
                type: 'or',
                fields: filter.getExpressionList().map(function (p) { return _this.timelessFilterToFilter(p); })
            };
        }
        else if (filter instanceof IsExpression) {
            var lhs = filter.operand, rhs = filter.expression;
            if (rhs instanceof LiteralExpression) {
                if (Set.isSetType(rhs.type)) {
                    return this.makeInFilter(lhs, rhs.value);
                }
                else {
                    return this.makeSelectorFilter(lhs, rhs.value);
                }
            }
            else {
                throw new Error("can not convert " + filter + " to Druid filter");
            }
        }
        else if (filter instanceof OverlapExpression) {
            var lhs_1 = filter.operand, rhs = filter.expression;
            if (rhs instanceof LiteralExpression) {
                var rhsType = rhs.type;
                if (rhsType === 'SET/STRING' || rhsType === 'SET/NUMBER' || rhsType === 'SET/NULL') {
                    return this.makeInFilter(lhs_1, rhs.value);
                }
                else if (Set.unwrapSetType(rhsType) === 'TIME_RANGE' && this.isTimeRef(lhs_1)) {
                    return this.makeIntervalFilter(lhs_1, rhs.value);
                }
                else if (rhsType === 'NUMBER_RANGE' || rhsType === 'TIME_RANGE' || rhsType === 'STRING_RANGE') {
                    return this.makeBoundFilter(lhs_1, rhs.value);
                }
                else if (rhsType === 'SET/NUMBER_RANGE' || rhsType === 'SET/TIME_RANGE' || rhsType === 'SET/STRING_RANGE') {
                    return {
                        type: "or",
                        fields: rhs.value.elements.map(function (range) { return _this.makeBoundFilter(lhs_1, range); })
                    };
                }
                else {
                    throw new Error("not supported OVERLAP rhs type " + rhsType);
                }
            }
            else {
                throw new Error("can not convert " + filter + " to Druid filter");
            }
        }
        else if (filter instanceof MatchExpression) {
            return this.makeRegexFilter(filter.operand, filter.regexp);
        }
        else if (filter instanceof ContainsExpression) {
            var lhs = filter.operand, rhs = filter.expression, compare = filter.compare;
            return this.makeContainsFilter(lhs, rhs, compare);
        }
        throw new Error("could not convert filter " + filter + " to Druid filter");
    };
    DruidFilterBuilder.prototype.makeJavaScriptFilter = function (ex) {
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo)
            throw new Error("can not construct JS filter on multiple");
        return {
            type: "javascript",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            "function": ex.getJSFn('d')
        };
    };
    DruidFilterBuilder.prototype.valueToIntervals = function (value) {
        if (isDate(value)) {
            return TimeRange.intervalFromDate(value);
        }
        else if (value instanceof TimeRange) {
            return value.toInterval();
        }
        else if (value instanceof Set) {
            return value.elements.map(function (v) {
                if (isDate(v)) {
                    return TimeRange.intervalFromDate(v);
                }
                else if (v instanceof TimeRange) {
                    return v.toInterval();
                }
                else {
                    throw new Error("can not convert set value " + JSON.stringify(v) + " to Druid interval");
                }
            });
        }
        else {
            throw new Error("can not convert " + JSON.stringify(value) + " to Druid intervals");
        }
    };
    DruidFilterBuilder.prototype.makeSelectorFilter = function (ex, value) {
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo) {
            return this.makeExpressionFilter(ex.is(r(value)));
        }
        if (attributeInfo.unsplitable) {
            throw new Error("can not convert " + ex + " = " + value + " to filter because it references an un-filterable metric '" + attributeInfo.name + "' which is most likely rolled up.");
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(ex);
        }
        catch (_a) {
            try {
                return this.makeExpressionFilter(ex.is(r(value)));
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(ex);
            }
        }
        if (value instanceof Range)
            value = value.start;
        var druidFilter = {
            type: "selector",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            value: value
        };
        if (extractionFn)
            druidFilter.extractionFn = extractionFn;
        return druidFilter;
    };
    DruidFilterBuilder.prototype.makeInFilter = function (ex, valueSet) {
        var _this = this;
        var elements = valueSet.elements;
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo) {
            var fields = elements.map(function (value) {
                return _this.makeSelectorFilter(ex, value);
            });
            return { type: "or", fields: fields };
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(ex);
        }
        catch (_a) {
            try {
                return this.makeExpressionFilter(ex.is(r(valueSet)));
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(ex);
            }
        }
        var inFilter = {
            type: 'in',
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            values: elements
        };
        if (extractionFn)
            inFilter.extractionFn = extractionFn;
        return inFilter;
    };
    DruidFilterBuilder.prototype.makeBoundFilter = function (ex, range) {
        var r0 = range.start;
        var r1 = range.end;
        var bounds = range.bounds;
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo) {
            return this.makeExpressionFilter(ex.overlap(range));
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(ex);
        }
        catch (_a) {
            try {
                return this.makeExpressionFilter(ex.overlap(range));
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(ex);
            }
        }
        var boundFilter = {
            type: "bound",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo)
        };
        if (extractionFn)
            boundFilter.extractionFn = extractionFn;
        if (range instanceof NumberRange || attributeInfo.nativeType === 'LONG') {
            boundFilter.ordering = 'numeric';
        }
        function dataToBound(d) {
            if (attributeInfo.nativeType === 'LONG') {
                return d.valueOf();
            }
            else {
                return d.toISOString();
            }
        }
        if (r0 != null) {
            boundFilter.lower = isDate(r0) ? dataToBound(r0) : r0;
            if (bounds[0] === '(')
                boundFilter.lowerStrict = true;
        }
        if (r1 != null) {
            boundFilter.upper = isDate(r1) ? dataToBound(r1) : r1;
            if (bounds[1] === ')')
                boundFilter.upperStrict = true;
        }
        return boundFilter;
    };
    DruidFilterBuilder.prototype.makeIntervalFilter = function (ex, range) {
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo) {
            return this.makeExpressionFilter(ex.overlap(range));
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(ex);
        }
        catch (_a) {
            try {
                return this.makeExpressionFilter(ex.overlap(range));
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(ex);
            }
        }
        var interval = this.valueToIntervals(range);
        var intervalFilter = {
            type: "interval",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            intervals: Array.isArray(interval) ? interval : [interval]
        };
        if (extractionFn)
            intervalFilter.extractionFn = extractionFn;
        return intervalFilter;
    };
    DruidFilterBuilder.prototype.makeRegexFilter = function (ex, regex) {
        var attributeInfo = this.getSingleReferenceAttributeInfo(ex);
        if (!attributeInfo) {
            return this.makeExpressionFilter(ex.match(regex));
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(ex);
        }
        catch (_a) {
            try {
                return this.makeExpressionFilter(ex.match(regex));
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(ex);
            }
        }
        var regexFilter = {
            type: "regex",
            dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
            pattern: regex
        };
        if (extractionFn)
            regexFilter.extractionFn = extractionFn;
        return regexFilter;
    };
    DruidFilterBuilder.prototype.makeContainsFilter = function (lhs, rhs, compare) {
        if (rhs instanceof LiteralExpression) {
            var attributeInfo = this.getSingleReferenceAttributeInfo(lhs);
            if (!attributeInfo) {
                return this.makeExpressionFilter(lhs.contains(rhs, compare));
            }
            if (lhs instanceof RefExpression && attributeInfo.termsDelegate) {
                return {
                    "type": "fullText",
                    "textColumn": this.getDimensionNameForAttributeInfo(attributeInfo),
                    "termsColumn": attributeInfo.termsDelegate,
                    "query": rhs.value,
                    "matchAll": true,
                    "usePrefixForLastTerm": true
                };
            }
            var extractionFn = void 0;
            try {
                extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(lhs);
            }
            catch (_a) {
                try {
                    return this.makeExpressionFilter(lhs.contains(rhs, compare));
                }
                catch (_b) {
                    extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(lhs);
                }
            }
            var searchFilter = {
                type: "search",
                dimension: this.getDimensionNameForAttributeInfo(attributeInfo),
                query: {
                    type: "contains",
                    value: rhs.value,
                    caseSensitive: compare === ContainsExpression.NORMAL
                }
            };
            if (extractionFn)
                searchFilter.extractionFn = extractionFn;
            return searchFilter;
        }
        else {
            return this.makeJavaScriptFilter(lhs.contains(rhs, compare));
        }
    };
    DruidFilterBuilder.prototype.makeExpressionFilter = function (filter) {
        var druidExpression = new DruidExpressionBuilder(this).expressionToDruidExpression(filter);
        if (druidExpression === null) {
            throw new Error("could not convert " + filter + " to Druid expression for filter");
        }
        return {
            type: "expression",
            expression: druidExpression
        };
    };
    DruidFilterBuilder.prototype.getSingleReferenceAttributeInfo = function (ex) {
        var freeReferences = ex.getFreeReferences();
        if (freeReferences.length !== 1)
            return null;
        var referenceName = freeReferences[0];
        return this.getAttributesInfo(referenceName);
    };
    DruidFilterBuilder.prototype.getDimensionNameForAttributeInfo = function (attributeInfo) {
        return attributeInfo.name === this.timeAttribute ? DruidFilterBuilder.TIME_ATTRIBUTE : attributeInfo.name;
    };
    DruidFilterBuilder.prototype.versionBefore = function (neededVersion) {
        var version = this.version;
        return version && External.versionLessThan(version, neededVersion);
    };
    DruidFilterBuilder.prototype.getAttributesInfo = function (attributeName) {
        return NamedArray.get(this.rawAttributes, attributeName);
    };
    DruidFilterBuilder.prototype.isTimeRef = function (ex) {
        return ex instanceof RefExpression && ex.name === this.timeAttribute;
    };
    DruidFilterBuilder.TIME_ATTRIBUTE = '__time';
    DruidFilterBuilder.TRUE_INTERVAL = "1000/3000";
    return DruidFilterBuilder;
}());
export { DruidFilterBuilder };
