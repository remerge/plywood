import * as tslib_1 from "tslib";
import * as hasOwnProp from 'has-own-prop';
import { Transform } from 'readable-stream';
import * as toArray from 'stream-to-array';
import { AttributeInfo, Range, Set, TimeRange } from '../datatypes/index';
import { $, ApplyExpression, CardinalityExpression, ChainableExpression, ChainableUnaryExpression, CountDistinctExpression, CountExpression, CustomAggregateExpression, Expression, FallbackExpression, FilterExpression, InExpression, IsExpression, LiteralExpression, MatchExpression, MaxExpression, MinExpression, NumberBucketExpression, OverlapExpression, RefExpression, SplitExpression, TimeBucketExpression, TimeFloorExpression, TimePartExpression, TimeShiftExpression } from '../expressions/index';
import { dictEqual, ExtendableError, nonEmptyLookup, shallowCopy } from '../helper/utils';
import { External } from './baseExternal';
import { DruidAggregationBuilder } from './utils/druidAggregationBuilder';
import { DruidExpressionBuilder } from './utils/druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './utils/druidExtractionFnBuilder';
import { DruidFilterBuilder } from './utils/druidFilterBuilder';
import { DruidHavingFilterBuilder } from './utils/druidHavingFilterBuilder';
var InvalidResultError = (function (_super) {
    tslib_1.__extends(InvalidResultError, _super);
    function InvalidResultError(message, result) {
        var _this = _super.call(this, message) || this;
        _this.result = result;
        return _this;
    }
    return InvalidResultError;
}(ExtendableError));
export { InvalidResultError };
function expressionNeedsNumericSort(ex) {
    var type = ex.type;
    return (type === 'NUMBER' || type === 'NUMBER_RANGE');
}
function simpleJSONEqual(a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
}
var DruidExternal = (function (_super) {
    tslib_1.__extends(DruidExternal, _super);
    function DruidExternal(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureEngine("druid");
        _this._ensureMinVersion("0.10.0");
        _this.timeAttribute = parameters.timeAttribute || DruidExternal.TIME_ATTRIBUTE;
        _this.customAggregations = parameters.customAggregations;
        _this.customTransforms = parameters.customTransforms;
        _this.allowEternity = parameters.allowEternity;
        _this.allowSelectQueries = parameters.allowSelectQueries;
        var introspectionStrategy = parameters.introspectionStrategy || DruidExternal.DEFAULT_INTROSPECTION_STRATEGY;
        if (DruidExternal.VALID_INTROSPECTION_STRATEGIES.indexOf(introspectionStrategy) === -1) {
            throw new Error("invalid introspectionStrategy '" + introspectionStrategy + "'");
        }
        _this.introspectionStrategy = introspectionStrategy;
        _this.exactResultsOnly = parameters.exactResultsOnly;
        _this.querySelection = parameters.querySelection;
        _this.context = parameters.context;
        return _this;
    }
    DruidExternal.fromJS = function (parameters, requester) {
        var value = External.jsToValue(parameters, requester);
        value.timeAttribute = parameters.timeAttribute;
        value.customAggregations = parameters.customAggregations || {};
        value.customTransforms = parameters.customTransforms || {};
        value.allowEternity = Boolean(parameters.allowEternity);
        value.allowSelectQueries = Boolean(parameters.allowSelectQueries);
        value.introspectionStrategy = parameters.introspectionStrategy;
        value.exactResultsOnly = Boolean(parameters.exactResultsOnly);
        value.querySelection = parameters.querySelection;
        value.context = parameters.context;
        return new DruidExternal(value);
    };
    DruidExternal.getSourceList = function (requester) {
        return toArray(requester({ query: { queryType: 'sourceList' } }))
            .then(function (sourcesArray) {
            var sources = sourcesArray[0];
            if (!Array.isArray(sources))
                throw new InvalidResultError('invalid sources response', sources);
            return sources.sort();
        });
    };
    DruidExternal.getVersion = function (requester) {
        return toArray(requester({
            query: {
                queryType: 'status'
            }
        }))
            .then(function (res) {
            return res[0].version;
        });
    };
    DruidExternal.isTimestampCompatibleSort = function (sort, label) {
        if (!sort)
            return true;
        var sortExpression = sort.expression;
        if (sortExpression instanceof RefExpression) {
            return sortExpression.name === label;
        }
        return false;
    };
    DruidExternal.timeBoundaryPostTransformFactory = function (applies) {
        return new Transform({
            objectMode: true,
            transform: function (d, encoding, callback) {
                if (applies) {
                    var datum = {};
                    for (var _i = 0, applies_1 = applies; _i < applies_1.length; _i++) {
                        var apply = applies_1[_i];
                        var name_1 = apply.name;
                        if (typeof d === 'string') {
                            datum[name_1] = new Date(d);
                        }
                        else {
                            if (apply.expression.op === 'max') {
                                datum[name_1] = new Date((d['maxIngestedEventTime'] || d['maxTime']));
                            }
                            else {
                                datum[name_1] = new Date(d['minTime']);
                            }
                        }
                    }
                    callback(null, {
                        type: 'datum',
                        datum: datum
                    });
                }
                else {
                    callback(null, {
                        type: 'value',
                        value: new Date((d['maxIngestedEventTime'] || d['maxTime'] || d['minTime']))
                    });
                }
            }
        });
    };
    DruidExternal.selectNextFactory = function (limit, descending) {
        var resultsSoFar = 0;
        return function (prevQuery, prevResultLength, prevMeta) {
            if (prevResultLength === 0)
                return null;
            var pagingIdentifiers = prevMeta.pagingIdentifiers;
            if (prevResultLength < prevQuery.pagingSpec.threshold)
                return null;
            resultsSoFar += prevResultLength;
            if (resultsSoFar >= limit)
                return null;
            pagingIdentifiers = DruidExternal.movePagingIdentifiers(pagingIdentifiers, descending ? -1 : 1);
            prevQuery.pagingSpec.pagingIdentifiers = pagingIdentifiers;
            prevQuery.pagingSpec.fromNext = false;
            prevQuery.pagingSpec.threshold = Math.min(limit - resultsSoFar, DruidExternal.SELECT_MAX_LIMIT);
            return prevQuery;
        };
    };
    DruidExternal.generateMaker = function (aggregation) {
        if (!aggregation)
            return null;
        var type = aggregation.type, fieldName = aggregation.fieldName;
        if (type === 'longSum' && fieldName === 'count') {
            return Expression._.count();
        }
        if (!fieldName) {
            var fieldNames = aggregation.fieldNames;
            if (!Array.isArray(fieldNames) || fieldNames.length !== 1)
                return null;
            fieldName = fieldNames[0];
        }
        var expression = $(fieldName);
        switch (type) {
            case "count":
                return Expression._.count();
            case "doubleSum":
            case "longSum":
                return Expression._.sum(expression);
            case "javascript":
                var fnAggregate = aggregation.fnAggregate, fnCombine = aggregation.fnCombine;
                if (fnAggregate !== fnCombine || fnCombine.indexOf('+') === -1)
                    return null;
                return Expression._.sum(expression);
            case "doubleMin":
            case "longMin":
                return Expression._.min(expression);
            case "doubleMax":
            case "longMax":
                return Expression._.max(expression);
            default:
                return null;
        }
    };
    DruidExternal.columnMetadataToRange = function (columnMetadata) {
        var minValue = columnMetadata.minValue, maxValue = columnMetadata.maxValue;
        if (minValue == null || maxValue == null)
            return null;
        return Range.fromJS({
            start: minValue,
            end: maxValue,
            bounds: '[]'
        });
    };
    DruidExternal.segmentMetadataPostProcess = function (timeAttribute, res) {
        var res0 = res[0];
        if (!res0 || !res0.columns)
            throw new InvalidResultError('malformed segmentMetadata response', res);
        var columns = res0.columns;
        var aggregators = res0.aggregators || {};
        var foundTime = false;
        var attributes = [];
        for (var name_2 in columns) {
            if (!hasOwnProp(columns, name_2))
                continue;
            var columnData = columns[name_2];
            if (columnData.errorMessage || columnData.size < 0)
                continue;
            if (name_2 === DruidExternal.TIME_ATTRIBUTE) {
                attributes.unshift(new AttributeInfo({
                    name: timeAttribute,
                    type: 'TIME',
                    nativeType: '__time',
                    cardinality: columnData.cardinality,
                    range: DruidExternal.columnMetadataToRange(columnData)
                }));
                foundTime = true;
            }
            else {
                if (name_2 === timeAttribute)
                    continue;
                var nativeType = columnData.type;
                switch (columnData.type) {
                    case 'DOUBLE':
                    case 'FLOAT':
                    case 'LONG':
                        attributes.push(new AttributeInfo({
                            name: name_2,
                            type: 'NUMBER',
                            nativeType: nativeType,
                            unsplitable: hasOwnProp(aggregators, name_2),
                            maker: DruidExternal.generateMaker(aggregators[name_2]),
                            cardinality: columnData.cardinality,
                            range: DruidExternal.columnMetadataToRange(columnData)
                        }));
                        break;
                    case 'STRING':
                        attributes.push(new AttributeInfo({
                            name: name_2,
                            type: columnData.hasMultipleValues ? 'SET/STRING' : 'STRING',
                            nativeType: nativeType,
                            cardinality: columnData.cardinality,
                            range: DruidExternal.columnMetadataToRange(columnData)
                        }));
                        break;
                    case 'hyperUnique':
                    case 'approximateHistogram':
                    case 'thetaSketch':
                    case 'HLLSketch':
                    case 'quantilesDoublesSketch':
                        attributes.push(new AttributeInfo({
                            name: name_2,
                            type: 'NULL',
                            nativeType: nativeType,
                            unsplitable: true
                        }));
                        break;
                    default:
                        attributes.push(new AttributeInfo({
                            name: name_2,
                            type: 'NULL',
                            nativeType: nativeType
                        }));
                        break;
                }
            }
        }
        if (!foundTime)
            throw new Error("no valid " + DruidExternal.TIME_ATTRIBUTE + " in segmentMetadata response");
        return attributes;
    };
    DruidExternal.introspectPostProcessFactory = function (timeAttribute, res) {
        var res0 = res[0];
        if (!Array.isArray(res0.dimensions) || !Array.isArray(res0.metrics)) {
            throw new InvalidResultError('malformed GET introspect response', res);
        }
        var attributes = [
            new AttributeInfo({ name: timeAttribute, type: 'TIME', nativeType: '__time' })
        ];
        res0.dimensions.forEach(function (dimension) {
            if (dimension === timeAttribute)
                return;
            attributes.push(new AttributeInfo({ name: dimension, type: 'STRING', nativeType: 'STRING' }));
        });
        res0.metrics.forEach(function (metric) {
            if (metric === timeAttribute)
                return;
            attributes.push(new AttributeInfo({ name: metric, type: 'NUMBER', nativeType: 'FLOAT', unsplitable: true }));
        });
        return attributes;
    };
    DruidExternal.movePagingIdentifiers = function (pagingIdentifiers, increment) {
        var newPagingIdentifiers = {};
        for (var key in pagingIdentifiers) {
            if (!hasOwnProp(pagingIdentifiers, key))
                continue;
            newPagingIdentifiers[key] = pagingIdentifiers[key] + increment;
        }
        return newPagingIdentifiers;
    };
    DruidExternal.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.timeAttribute = this.timeAttribute;
        value.customAggregations = this.customAggregations;
        value.customTransforms = this.customTransforms;
        value.allowEternity = this.allowEternity;
        value.allowSelectQueries = this.allowSelectQueries;
        value.introspectionStrategy = this.introspectionStrategy;
        value.exactResultsOnly = this.exactResultsOnly;
        value.querySelection = this.querySelection;
        value.context = this.context;
        return value;
    };
    DruidExternal.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        if (this.timeAttribute !== DruidExternal.TIME_ATTRIBUTE)
            js.timeAttribute = this.timeAttribute;
        if (nonEmptyLookup(this.customAggregations))
            js.customAggregations = this.customAggregations;
        if (nonEmptyLookup(this.customTransforms))
            js.customTransforms = this.customTransforms;
        if (this.allowEternity)
            js.allowEternity = true;
        if (this.allowSelectQueries)
            js.allowSelectQueries = true;
        if (this.introspectionStrategy !== DruidExternal.DEFAULT_INTROSPECTION_STRATEGY)
            js.introspectionStrategy = this.introspectionStrategy;
        if (this.exactResultsOnly)
            js.exactResultsOnly = true;
        if (this.querySelection)
            js.querySelection = this.querySelection;
        if (this.context)
            js.context = this.context;
        return js;
    };
    DruidExternal.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.timeAttribute === other.timeAttribute &&
            simpleJSONEqual(this.customAggregations, other.customAggregations) &&
            simpleJSONEqual(this.customTransforms, other.customTransforms) &&
            this.allowEternity === other.allowEternity &&
            this.allowSelectQueries === other.allowSelectQueries &&
            this.introspectionStrategy === other.introspectionStrategy &&
            this.exactResultsOnly === other.exactResultsOnly &&
            this.querySelection === other.querySelection &&
            dictEqual(this.context, other.context);
    };
    DruidExternal.prototype.canHandleFilter = function (filter) {
        return !filter.expression.some(function (ex) { return ex.isOp('cardinality') ? true : null; });
    };
    DruidExternal.prototype.canHandleSort = function (sort) {
        if (this.mode === 'raw') {
            if (sort.refName() !== this.timeAttribute)
                return false;
            return sort.direction === 'ascending';
        }
        else {
            return true;
        }
    };
    DruidExternal.prototype.getQuerySelection = function () {
        return this.querySelection || 'any';
    };
    DruidExternal.prototype.getDruidDataSource = function () {
        var source = this.source;
        if (Array.isArray(source)) {
            return {
                type: "union",
                dataSources: source
            };
        }
        else {
            return source;
        }
    };
    DruidExternal.prototype.isTimeRef = function (ex) {
        return ex instanceof RefExpression && ex.name === this.timeAttribute;
    };
    DruidExternal.prototype.splitExpressionToGranularityInflater = function (splitExpression, label) {
        if (this.isTimeRef(splitExpression)) {
            return {
                granularity: 'none',
                inflater: External.timeInflaterFactory(label)
            };
        }
        else if (splitExpression instanceof TimeBucketExpression || splitExpression instanceof TimeFloorExpression) {
            var operand = splitExpression.operand, duration = splitExpression.duration;
            var timezone = splitExpression.getTimezone();
            if (this.isTimeRef(operand)) {
                return {
                    granularity: {
                        type: "period",
                        period: duration.toString(),
                        timeZone: timezone.toString()
                    },
                    inflater: External.getInteligentInflater(splitExpression, label)
                };
            }
        }
        return null;
    };
    DruidExternal.prototype.makeOutputName = function (name) {
        if (name.indexOf('__') === 0) {
            return '***' + name;
        }
        return name;
    };
    DruidExternal.prototype.topNCompatibleSort = function () {
        var _this = this;
        var sort = this.sort;
        if (!sort)
            return true;
        var refExpression = sort.expression;
        if (refExpression instanceof RefExpression) {
            var sortRefName_1 = refExpression.name;
            var sortApply = this.applies.find(function (apply) { return apply.name === sortRefName_1; });
            if (sortApply) {
                return !sortApply.expression.some(function (ex) {
                    if (ex instanceof FilterExpression) {
                        return ex.expression.some(function (ex) { return _this.isTimeRef(ex) || null; });
                    }
                    return null;
                });
            }
        }
        return true;
    };
    DruidExternal.prototype.expressionToDimensionInflater = function (expression, label) {
        var _this = this;
        var freeReferences = expression.getFreeReferences();
        if (freeReferences.length === 0) {
            return {
                dimension: {
                    type: "extraction",
                    dimension: DruidExternal.TIME_ATTRIBUTE,
                    outputName: this.makeOutputName(label),
                    extractionFn: new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(expression)
                },
                inflater: null
            };
        }
        var makeExpression = function () {
            var druidExpression = new DruidExpressionBuilder(_this).expressionToDruidExpression(expression);
            if (druidExpression === null) {
                throw new Error("could not convert " + expression + " to Druid expression");
            }
            var outputName = _this.makeOutputName(label);
            var outputType = DruidExpressionBuilder.expressionTypeToOutputType(expression.type);
            var inflater = External.getInteligentInflater(expression, label);
            var dimensionSrcName = outputName;
            var virtualColumn = null;
            if (!(expression instanceof RefExpression)) {
                dimensionSrcName = 'v:' + dimensionSrcName;
                virtualColumn = {
                    type: "expression",
                    name: dimensionSrcName,
                    expression: druidExpression,
                    outputType: outputType
                };
            }
            return {
                virtualColumn: virtualColumn,
                dimension: {
                    type: "default",
                    dimension: dimensionSrcName,
                    outputName: outputName,
                    outputType: outputType
                },
                inflater: inflater
            };
        };
        function isComplexFallback(expression) {
            if (expression instanceof FallbackExpression) {
                if (!expression.expression.isOp('ref'))
                    return false;
                var myOp = expression.operand;
                return (myOp instanceof ChainableExpression && myOp.operand instanceof ChainableExpression);
            }
            return false;
        }
        if (freeReferences.length > 1 || expression.some(function (ex) { return ex.isOp('then') || null; }) || isComplexFallback(expression)) {
            return makeExpression();
        }
        var referenceName = freeReferences[0];
        var attributeInfo = this.getAttributesInfo(referenceName);
        if (attributeInfo.unsplitable) {
            throw new Error("can not convert " + expression + " to split because it references an un-splitable metric '" + referenceName + "' which is most likely rolled up.");
        }
        var extractionFn;
        try {
            extractionFn = new DruidExtractionFnBuilder(this, false).expressionToExtractionFn(expression);
        }
        catch (_a) {
            try {
                return makeExpression();
            }
            catch (_b) {
                extractionFn = new DruidExtractionFnBuilder(this, true).expressionToExtractionFn(expression);
            }
        }
        var simpleInflater = External.getInteligentInflater(expression, label);
        var dimension = {
            type: "default",
            dimension: attributeInfo.name === this.timeAttribute ? DruidExternal.TIME_ATTRIBUTE : attributeInfo.name,
            outputName: this.makeOutputName(label)
        };
        if (extractionFn) {
            dimension.type = "extraction";
            dimension.extractionFn = extractionFn;
        }
        if (expression.type === 'NUMBER') {
            dimension.outputType = dimension.dimension === DruidExternal.TIME_ATTRIBUTE ? 'LONG' : 'FLOAT';
        }
        if (expression instanceof RefExpression || expression instanceof TimeBucketExpression || expression instanceof TimePartExpression || expression instanceof NumberBucketExpression) {
            return {
                dimension: dimension,
                inflater: simpleInflater
            };
        }
        if (expression instanceof CardinalityExpression) {
            return {
                dimension: dimension,
                inflater: External.setCardinalityInflaterFactory(label)
            };
        }
        var effectiveType = Set.unwrapSetType(expression.type);
        if (simpleInflater || effectiveType === 'STRING' || effectiveType === 'NULL') {
            return {
                dimension: dimension,
                inflater: simpleInflater
            };
        }
        throw new Error("could not convert " + expression + " to a Druid dimension");
    };
    DruidExternal.prototype.expressionToDimensionInflaterHaving = function (expression, label, havingFilter) {
        var dimensionInflater = this.expressionToDimensionInflater(expression, label);
        dimensionInflater.having = havingFilter;
        if (expression.type !== 'SET/STRING')
            return dimensionInflater;
        var _a = havingFilter.extractFromAnd(function (hf) {
            if (hf instanceof ChainableExpression) {
                var hfOp = hf.op;
                var hfOperand = hf.operand;
                if (hfOperand instanceof RefExpression && hfOperand.name === label) {
                    if (hfOp === 'match')
                        return true;
                    if (hfOp === 'is')
                        return hf.expression.isOp('literal');
                }
            }
            return false;
        }), extract = _a.extract, rest = _a.rest;
        if (extract.equals(Expression.TRUE))
            return dimensionInflater;
        if (extract instanceof MatchExpression) {
            return {
                dimension: {
                    type: "regexFiltered",
                    delegate: dimensionInflater.dimension,
                    pattern: extract.regexp
                },
                inflater: dimensionInflater.inflater,
                having: rest
            };
        }
        else if (extract instanceof IsExpression) {
            var value = extract.expression.getLiteralValue();
            return {
                dimension: {
                    type: "listFiltered",
                    delegate: dimensionInflater.dimension,
                    values: Set.isSet(value) ? value.elements : [value]
                },
                inflater: dimensionInflater.inflater,
                having: rest
            };
        }
        else if (extract instanceof InExpression) {
            return {
                dimension: {
                    type: "listFiltered",
                    delegate: dimensionInflater.dimension,
                    values: extract.expression.getLiteralValue().elements
                },
                inflater: dimensionInflater.inflater,
                having: rest
            };
        }
        return dimensionInflater;
    };
    DruidExternal.prototype.splitToDruid = function (split) {
        var _this = this;
        var leftoverHavingFilter = this.havingFilter;
        var selectedAttributes = this.getSelectedAttributes();
        if (this.getQuerySelection() === 'group-by-only' || split.isMultiSplit()) {
            var timestampLabel = null;
            var granularity = null;
            var virtualColumns_1 = [];
            var dimensions_1 = [];
            var inflaters_1 = [];
            split.mapSplits(function (name, expression) {
                var _a = _this.expressionToDimensionInflaterHaving(expression, name, leftoverHavingFilter), virtualColumn = _a.virtualColumn, dimension = _a.dimension, inflater = _a.inflater, having = _a.having;
                leftoverHavingFilter = having;
                if (virtualColumn)
                    virtualColumns_1.push(virtualColumn);
                dimensions_1.push(dimension);
                if (inflater) {
                    inflaters_1.push(inflater);
                }
            });
            return {
                queryType: 'groupBy',
                virtualColumns: virtualColumns_1,
                dimensions: dimensions_1,
                timestampLabel: timestampLabel,
                granularity: granularity || 'all',
                leftoverHavingFilter: leftoverHavingFilter,
                postTransform: External.postTransformFactory(inflaters_1, selectedAttributes, split.mapSplits(function (name) { return name; }), null)
            };
        }
        var splitExpression = split.firstSplitExpression();
        var label = split.firstSplitName();
        if (!this.limit && DruidExternal.isTimestampCompatibleSort(this.sort, label)) {
            var granularityInflater = this.splitExpressionToGranularityInflater(splitExpression, label);
            if (granularityInflater) {
                return {
                    queryType: 'timeseries',
                    granularity: granularityInflater.granularity,
                    leftoverHavingFilter: leftoverHavingFilter,
                    timestampLabel: label,
                    postTransform: External.postTransformFactory([granularityInflater.inflater], selectedAttributes, [label], null)
                };
            }
        }
        var dimensionInflater = this.expressionToDimensionInflaterHaving(splitExpression, label, leftoverHavingFilter);
        leftoverHavingFilter = dimensionInflater.having;
        var inflaters = [dimensionInflater.inflater].filter(Boolean);
        if (leftoverHavingFilter.equals(Expression.TRUE) &&
            (this.limit || split.maxBucketNumber() < 1000) &&
            !this.exactResultsOnly &&
            this.topNCompatibleSort() &&
            this.getQuerySelection() === 'any') {
            return {
                queryType: 'topN',
                virtualColumns: dimensionInflater.virtualColumn ? [dimensionInflater.virtualColumn] : null,
                dimension: dimensionInflater.dimension,
                granularity: 'all',
                leftoverHavingFilter: leftoverHavingFilter,
                timestampLabel: null,
                postTransform: External.postTransformFactory(inflaters, selectedAttributes, [label], null)
            };
        }
        return {
            queryType: 'groupBy',
            virtualColumns: dimensionInflater.virtualColumn ? [dimensionInflater.virtualColumn] : null,
            dimensions: [dimensionInflater.dimension],
            granularity: 'all',
            leftoverHavingFilter: leftoverHavingFilter,
            timestampLabel: null,
            postTransform: External.postTransformFactory(inflaters, selectedAttributes, [label], null)
        };
    };
    DruidExternal.prototype.isMinMaxTimeExpression = function (applyExpression) {
        if (applyExpression instanceof MinExpression || applyExpression instanceof MaxExpression) {
            return this.isTimeRef(applyExpression.expression);
        }
        else {
            return false;
        }
    };
    DruidExternal.prototype.getTimeBoundaryQueryAndPostTransform = function () {
        var _a = this, mode = _a.mode, context = _a.context;
        var druidQuery = {
            queryType: "timeBoundary",
            dataSource: this.getDruidDataSource()
        };
        if (context) {
            druidQuery.context = context;
        }
        var applies = null;
        if (mode === 'total') {
            applies = this.applies;
            if (applies.length === 1) {
                var loneApplyExpression = applies[0].expression;
                druidQuery.bound = loneApplyExpression.op + "Time";
            }
        }
        else if (mode === 'value') {
            var valueExpression = this.valueExpression;
            druidQuery.bound = valueExpression.op + "Time";
        }
        else {
            throw new Error("invalid mode '" + mode + "' for timeBoundary");
        }
        return {
            query: druidQuery,
            context: { timestamp: null },
            postTransform: DruidExternal.timeBoundaryPostTransformFactory(applies)
        };
    };
    DruidExternal.prototype.nestedGroupByIfNeeded = function () {
        var parseResplitAgg = function (applyExpression) {
            var resplitAgg = applyExpression;
            if (!(resplitAgg instanceof ChainableExpression) || !resplitAgg.isAggregate())
                return null;
            var resplitApply = resplitAgg.operand;
            if (!(resplitApply instanceof ApplyExpression))
                return null;
            var resplitSplit = resplitApply.operand;
            if (!(resplitSplit instanceof SplitExpression))
                return null;
            var resplitRefOrFilter = resplitSplit.operand;
            var resplitRef;
            var effectiveResplitApply = resplitApply.changeOperand(Expression._);
            if (resplitRefOrFilter instanceof FilterExpression) {
                resplitRef = resplitRefOrFilter.operand;
                var filterExpression_1 = resplitRefOrFilter.expression;
                effectiveResplitApply = effectiveResplitApply.changeExpression(effectiveResplitApply.expression.substitute(function (ex) {
                    if (ex instanceof RefExpression && ex.type === 'DATASET') {
                        return ex.filter(filterExpression_1);
                    }
                    return null;
                }));
            }
            else {
                resplitRef = resplitRefOrFilter;
            }
            if (!(resplitRef instanceof RefExpression))
                return null;
            return {
                resplitAgg: resplitAgg.changeOperand(Expression._),
                resplitApply: effectiveResplitApply,
                resplitSplit: resplitSplit.changeOperand(Expression._)
            };
        };
        var divvyUpNestedSplitExpression = function (splitExpression, intermediateName) {
            if (splitExpression instanceof TimeBucketExpression || splitExpression instanceof NumberBucketExpression) {
                return {
                    inner: splitExpression,
                    outer: splitExpression.changeOperand($(intermediateName))
                };
            }
            else {
                return {
                    inner: splitExpression,
                    outer: $(intermediateName)
                };
            }
        };
        var _a = this, applies = _a.applies, split = _a.split;
        var effectiveApplies = applies ? applies : [Expression._.apply('__VALUE__', this.valueExpression)];
        if (!effectiveApplies.some(function (apply) {
            return apply.expression.some(function (ex) { return ex instanceof SplitExpression ? true : null; });
        }))
            return null;
        var globalResplitSplit = null;
        var outerAttributes = [];
        var innerApplies = [];
        var outerApplies = effectiveApplies.map(function (apply, i) {
            var c = 0;
            return apply.changeExpression(apply.expression.substitute(function (ex) {
                if (ex.isAggregate()) {
                    var resplit = parseResplitAgg(ex);
                    if (resplit) {
                        if (globalResplitSplit) {
                            if (!globalResplitSplit.equals(resplit.resplitSplit))
                                throw new Error('all resplit aggregators must have the same split');
                        }
                        else {
                            globalResplitSplit = resplit.resplitSplit;
                        }
                        var resplitApply = resplit.resplitApply;
                        var oldName_1 = resplitApply.name;
                        var newName_1 = oldName_1 + '_' + i;
                        innerApplies.push(resplitApply
                            .changeName(newName_1)
                            .changeExpression(resplitApply.expression.setOption('forceFinalize', true)));
                        outerAttributes.push(AttributeInfo.fromJS({ name: newName_1, type: 'NUMBER' }));
                        return resplit.resplitAgg.substitute(function (ex) {
                            if (ex instanceof RefExpression && ex.name === oldName_1) {
                                return ex.changeName(newName_1);
                            }
                            return null;
                        });
                    }
                    else {
                        var tempName = "a" + i + "_" + c++;
                        innerApplies.push(Expression._.apply(tempName, ex));
                        outerAttributes.push(AttributeInfo.fromJS({
                            name: tempName,
                            type: ex.type,
                            nativeType: (ex instanceof CountDistinctExpression) ? 'hyperUnique' : null
                        }));
                        if (ex instanceof CountExpression) {
                            return Expression._.sum($(tempName));
                        }
                        else if (ex instanceof ChainableUnaryExpression) {
                            return ex.changeOperand(Expression._).changeExpression($(tempName));
                        }
                        else if (ex instanceof CustomAggregateExpression) {
                            throw new Error('can not currently combine custom aggregation and re-split');
                        }
                        else {
                            throw new Error("bad '" + ex.op + "' aggregate in custom expression");
                        }
                    }
                }
                return null;
            }));
        });
        if (!globalResplitSplit)
            return null;
        var outerSplits = {};
        var innerSplits = {};
        var splitCount = 0;
        globalResplitSplit.mapSplits(function (name, ex) {
            var outerSplitName = null;
            if (split) {
                split.mapSplits(function (name, myEx) {
                    if (ex.equals(myEx)) {
                        outerSplitName = name;
                    }
                });
            }
            var intermediateName = "s" + splitCount++;
            var divvy = divvyUpNestedSplitExpression(ex, intermediateName);
            outerAttributes.push(AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }));
            innerSplits[intermediateName] = divvy.inner;
            if (outerSplitName) {
                outerSplits[outerSplitName] = divvy.outer;
            }
        });
        if (split) {
            split.mapSplits(function (name, ex) {
                if (outerSplits[name])
                    return;
                var intermediateName = "s" + splitCount++;
                var divvy = divvyUpNestedSplitExpression(ex, intermediateName);
                innerSplits[intermediateName] = divvy.inner;
                outerAttributes.push(AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }));
                outerSplits[name] = divvy.outer;
            });
        }
        var innerValue = this.valueOf();
        innerValue.mode = 'split';
        innerValue.applies = innerApplies;
        innerValue.querySelection = 'group-by-only';
        innerValue.split = split ? split.changeSplits(innerSplits) : Expression._.split(innerSplits);
        innerValue.limit = null;
        innerValue.sort = null;
        var innerExternal = new DruidExternal(innerValue);
        var innerQuery = innerExternal.getQueryAndPostTransform().query;
        delete innerQuery.context;
        var outerValue = this.valueOf();
        outerValue.rawAttributes = outerAttributes;
        if (applies) {
            outerValue.applies = outerApplies;
        }
        else {
            outerValue.valueExpression = outerApplies[0].expression;
        }
        outerValue.filter = Expression.TRUE;
        outerValue.allowEternity = true;
        outerValue.querySelection = 'group-by-only';
        if (split)
            outerValue.split = split.changeSplits(outerSplits);
        var outerExternal = new DruidExternal(outerValue);
        var outerQueryAndPostTransform = outerExternal.getQueryAndPostTransform();
        outerQueryAndPostTransform.query.dataSource = {
            type: 'query',
            query: innerQuery
        };
        return outerQueryAndPostTransform;
    };
    DruidExternal.prototype.getQueryAndPostTransform = function () {
        var _this = this;
        var _a = this, mode = _a.mode, applies = _a.applies, sort = _a.sort, limit = _a.limit, context = _a.context, querySelection = _a.querySelection;
        if (querySelection !== 'group-by-only') {
            if (mode === 'total' && applies && applies.length && applies.every(function (apply) { return _this.isMinMaxTimeExpression(apply.expression); })) {
                return this.getTimeBoundaryQueryAndPostTransform();
            }
            else if (mode === 'value' && this.isMinMaxTimeExpression(this.valueExpression)) {
                return this.getTimeBoundaryQueryAndPostTransform();
            }
        }
        var druidQuery = {
            queryType: 'timeseries',
            dataSource: this.getDruidDataSource(),
            intervals: null,
            granularity: 'all'
        };
        var requesterContext = {
            timestamp: null,
            ignorePrefix: '!',
            dummyPrefix: '***'
        };
        if (context) {
            druidQuery.context = shallowCopy(context);
        }
        var filterAndIntervals = new DruidFilterBuilder(this).filterToDruid(this.getQueryFilter());
        druidQuery.intervals = filterAndIntervals.intervals;
        if (filterAndIntervals.filter) {
            druidQuery.filter = filterAndIntervals.filter;
        }
        var aggregationsAndPostAggregations;
        switch (mode) {
            case 'raw':
                if (!this.allowSelectQueries) {
                    throw new Error("to issue 'scan' or 'select' queries allowSelectQueries flag must be set");
                }
                var derivedAttributes_1 = this.derivedAttributes;
                var selectedAttributes = this.getSelectedAttributes();
                if (this.versionBefore('0.11.0')) {
                    var selectDimensions_1 = [];
                    var selectMetrics_1 = [];
                    var inflaters_2 = [];
                    var timeAttribute_1 = this.timeAttribute;
                    selectedAttributes.forEach(function (attribute) {
                        var name = attribute.name, type = attribute.type, nativeType = attribute.nativeType, unsplitable = attribute.unsplitable;
                        if (name === timeAttribute_1) {
                            requesterContext.timestamp = name;
                        }
                        else {
                            if (nativeType === 'STRING' || (!nativeType && !unsplitable)) {
                                var derivedAttribute = derivedAttributes_1[name];
                                if (derivedAttribute) {
                                    var dimensionInflater = _this.expressionToDimensionInflater(derivedAttribute, name);
                                    selectDimensions_1.push(dimensionInflater.dimension);
                                    if (dimensionInflater.inflater)
                                        inflaters_2.push(dimensionInflater.inflater);
                                    return;
                                }
                                else {
                                    selectDimensions_1.push(name);
                                }
                            }
                            else {
                                selectMetrics_1.push(name);
                            }
                        }
                        switch (type) {
                            case 'BOOLEAN':
                                inflaters_2.push(External.booleanInflaterFactory(name));
                                break;
                            case 'NUMBER':
                                inflaters_2.push(External.numberInflaterFactory(name));
                                break;
                            case 'TIME':
                                inflaters_2.push(External.timeInflaterFactory(name));
                                break;
                            case 'SET/STRING':
                                inflaters_2.push(External.setStringInflaterFactory(name));
                                break;
                        }
                    });
                    if (!selectDimensions_1.length)
                        selectDimensions_1.push(DruidExternal.DUMMY_NAME);
                    if (!selectMetrics_1.length)
                        selectMetrics_1.push(DruidExternal.DUMMY_NAME);
                    var resultLimit = limit ? limit.value : Infinity;
                    druidQuery.queryType = 'select';
                    druidQuery.dimensions = selectDimensions_1;
                    druidQuery.metrics = selectMetrics_1;
                    druidQuery.pagingSpec = {
                        "pagingIdentifiers": {},
                        "threshold": Math.min(resultLimit, DruidExternal.SELECT_INIT_LIMIT)
                    };
                    var descending = sort && sort.direction === 'descending';
                    if (descending) {
                        druidQuery.descending = true;
                    }
                    return {
                        query: druidQuery,
                        context: requesterContext,
                        postTransform: External.postTransformFactory(inflaters_2, selectedAttributes.map(function (a) { return a.dropOriginInfo(); }), null, null),
                        next: DruidExternal.selectNextFactory(resultLimit, descending)
                    };
                }
                var virtualColumns_2 = [];
                var columns_1 = [];
                var inflaters_3 = [];
                selectedAttributes.forEach(function (attribute) {
                    var name = attribute.name, type = attribute.type, nativeType = attribute.nativeType, unsplitable = attribute.unsplitable;
                    if (nativeType === '__time' && name !== '__time') {
                        virtualColumns_2.push({
                            type: "expression",
                            name: name,
                            expression: "__time",
                            outputType: "STRING"
                        });
                    }
                    else {
                        var derivedAttribute = derivedAttributes_1[name];
                        if (derivedAttribute) {
                            var druidExpression = new DruidExpressionBuilder(_this).expressionToDruidExpression(derivedAttribute);
                            if (druidExpression === null) {
                                throw new Error("could not convert " + derivedAttribute + " to Druid expression");
                            }
                            virtualColumns_2.push({
                                type: "expression",
                                name: name,
                                expression: druidExpression,
                                outputType: "STRING"
                            });
                        }
                    }
                    columns_1.push(name);
                    switch (type) {
                        case 'BOOLEAN':
                            inflaters_3.push(External.booleanInflaterFactory(name));
                            break;
                        case 'NUMBER':
                            inflaters_3.push(External.numberInflaterFactory(name));
                            break;
                        case 'TIME':
                            inflaters_3.push(External.timeInflaterFactory(name));
                            break;
                        case 'SET/STRING':
                            inflaters_3.push(External.setStringInflaterFactory(name));
                            break;
                    }
                });
                druidQuery.queryType = 'scan';
                druidQuery.resultFormat = 'compactedList';
                if (virtualColumns_2.length)
                    druidQuery.virtualColumns = virtualColumns_2;
                druidQuery.columns = columns_1;
                if (limit)
                    druidQuery.limit = limit.value;
                return {
                    query: druidQuery,
                    context: requesterContext,
                    postTransform: External.postTransformFactory(inflaters_3, selectedAttributes.map(function (a) { return a.dropOriginInfo(); }), null, null)
                };
            case 'value':
                var nestedGroupByValue = this.nestedGroupByIfNeeded();
                if (nestedGroupByValue)
                    return nestedGroupByValue;
                aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations([this.toValueApply()]);
                if (aggregationsAndPostAggregations.aggregations.length) {
                    druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
                }
                if (aggregationsAndPostAggregations.postAggregations.length) {
                    druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
                }
                if (querySelection === 'group-by-only') {
                    druidQuery.queryType = 'groupBy';
                    druidQuery.dimensions = [];
                }
                return {
                    query: druidQuery,
                    context: requesterContext,
                    postTransform: External.valuePostTransformFactory()
                };
            case 'total':
                var nestedGroupByTotal = this.nestedGroupByIfNeeded();
                if (nestedGroupByTotal)
                    return nestedGroupByTotal;
                aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations(this.applies);
                if (aggregationsAndPostAggregations.aggregations.length) {
                    druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
                }
                if (aggregationsAndPostAggregations.postAggregations.length) {
                    druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
                }
                if (querySelection === 'group-by-only') {
                    druidQuery.queryType = 'groupBy';
                    druidQuery.dimensions = [];
                }
                return {
                    query: druidQuery,
                    context: requesterContext,
                    postTransform: External.postTransformFactory([], this.getSelectedAttributes(), [], applies)
                };
            case 'split':
                var nestedGroupBy = this.nestedGroupByIfNeeded();
                if (nestedGroupBy)
                    return nestedGroupBy;
                var split = this.getQuerySplit();
                var splitSpec = this.splitToDruid(split);
                druidQuery.queryType = splitSpec.queryType;
                druidQuery.granularity = splitSpec.granularity;
                if (splitSpec.virtualColumns && splitSpec.virtualColumns.length)
                    druidQuery.virtualColumns = splitSpec.virtualColumns;
                if (splitSpec.dimension)
                    druidQuery.dimension = splitSpec.dimension;
                if (splitSpec.dimensions)
                    druidQuery.dimensions = splitSpec.dimensions;
                var leftoverHavingFilter = splitSpec.leftoverHavingFilter;
                var timestampLabel = splitSpec.timestampLabel;
                requesterContext.timestamp = timestampLabel;
                var postTransform = splitSpec.postTransform;
                aggregationsAndPostAggregations = new DruidAggregationBuilder(this).makeAggregationsAndPostAggregations(applies);
                if (aggregationsAndPostAggregations.aggregations.length) {
                    druidQuery.aggregations = aggregationsAndPostAggregations.aggregations;
                }
                if (aggregationsAndPostAggregations.postAggregations.length) {
                    druidQuery.postAggregations = aggregationsAndPostAggregations.postAggregations;
                }
                switch (druidQuery.queryType) {
                    case 'timeseries':
                        if (sort) {
                            if (!split.hasKey(sort.refName())) {
                                throw new Error('can not sort within timeseries query');
                            }
                            if (sort.direction === 'descending')
                                druidQuery.descending = true;
                        }
                        if (limit) {
                            throw new Error('can not limit within timeseries query');
                        }
                        if (!druidQuery.context || !hasOwnProp(druidQuery.context, 'skipEmptyBuckets')) {
                            druidQuery.context = druidQuery.context || {};
                            druidQuery.context.skipEmptyBuckets = "true";
                        }
                        break;
                    case 'topN':
                        var metric = void 0;
                        if (sort) {
                            var inverted = void 0;
                            if (this.sortOnLabel()) {
                                if (expressionNeedsNumericSort(split.firstSplitExpression())) {
                                    metric = { type: 'dimension', ordering: 'numeric' };
                                }
                                else {
                                    metric = { type: 'dimension', ordering: 'lexicographic' };
                                }
                                inverted = sort.direction === 'descending';
                            }
                            else {
                                metric = sort.refName();
                                inverted = sort.direction === 'ascending';
                            }
                            if (inverted) {
                                metric = { type: "inverted", metric: metric };
                            }
                        }
                        else {
                            metric = { type: 'dimension', ordering: 'lexicographic' };
                        }
                        druidQuery.metric = metric;
                        druidQuery.threshold = limit ? limit.value : 1000;
                        break;
                    case 'groupBy':
                        var orderByColumn = null;
                        if (sort) {
                            var col = sort.refName();
                            orderByColumn = {
                                dimension: this.makeOutputName(col),
                                direction: sort.direction
                            };
                            if (this.sortOnLabel()) {
                                if (expressionNeedsNumericSort(split.splits[col])) {
                                    orderByColumn.dimensionOrder = 'numeric';
                                }
                            }
                            druidQuery.limitSpec = {
                                type: "default",
                                columns: [orderByColumn]
                            };
                        }
                        if (limit) {
                            if (!druidQuery.limitSpec) {
                                druidQuery.limitSpec = {
                                    type: "default",
                                    columns: [this.makeOutputName(split.firstSplitName())]
                                };
                            }
                            druidQuery.limitSpec.limit = limit.value;
                        }
                        if (!leftoverHavingFilter.equals(Expression.TRUE)) {
                            druidQuery.having = new DruidHavingFilterBuilder(this).filterToHavingFilter(leftoverHavingFilter);
                        }
                        break;
                }
                return {
                    query: druidQuery,
                    context: requesterContext,
                    postTransform: postTransform
                };
            default:
                throw new Error("can not get query for: " + this.mode);
        }
    };
    DruidExternal.prototype.getIntrospectAttributesWithSegmentMetadata = function (depth) {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var _a, requester, timeAttribute, context, analysisTypes, query, res, attributes, resTB, resTB0, e_1;
            return tslib_1.__generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _a = this, requester = _a.requester, timeAttribute = _a.timeAttribute, context = _a.context;
                        analysisTypes = ['aggregators'];
                        if (depth === 'deep') {
                            analysisTypes.push('cardinality', 'minmax');
                        }
                        query = {
                            queryType: 'segmentMetadata',
                            dataSource: this.getDruidDataSource(),
                            merge: true,
                            analysisTypes: analysisTypes,
                            lenientAggregatorMerge: true
                        };
                        if (context) {
                            query.context = context;
                        }
                        return [4, toArray(requester({ query: query }))];
                    case 1:
                        res = _b.sent();
                        attributes = DruidExternal.segmentMetadataPostProcess(timeAttribute, res);
                        if (!(depth !== 'shallow' && attributes.length && attributes[0].nativeType === '__time' && !attributes[0].range)) return [3, 5];
                        _b.label = 2;
                    case 2:
                        _b.trys.push([2, 4, , 5]);
                        query = {
                            queryType: "timeBoundary",
                            dataSource: this.getDruidDataSource()
                        };
                        if (context) {
                            query.context = context;
                        }
                        return [4, toArray(requester({ query: query }))];
                    case 3:
                        resTB = _b.sent();
                        resTB0 = resTB[0];
                        attributes[0] = attributes[0].changeRange(TimeRange.fromJS({
                            start: resTB0.minTime,
                            end: resTB0.maxTime,
                            bounds: '[]'
                        }));
                        return [3, 5];
                    case 4:
                        e_1 = _b.sent();
                        return [3, 5];
                    case 5: return [2, attributes];
                }
            });
        });
    };
    DruidExternal.prototype.getIntrospectAttributesWithGet = function () {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var _a, requester, timeAttribute, res;
            return tslib_1.__generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _a = this, requester = _a.requester, timeAttribute = _a.timeAttribute;
                        return [4, toArray(requester({
                                query: {
                                    queryType: 'introspect',
                                    dataSource: this.getDruidDataSource()
                                }
                            }))];
                    case 1:
                        res = _b.sent();
                        return [2, DruidExternal.introspectPostProcessFactory(timeAttribute, res)];
                }
            });
        });
    };
    DruidExternal.prototype.getIntrospectAttributes = function (depth) {
        var _this = this;
        switch (this.introspectionStrategy) {
            case 'segment-metadata-fallback':
                return this.getIntrospectAttributesWithSegmentMetadata(depth)
                    .catch(function (err) {
                    if (err.message.indexOf("querySegmentSpec can't be null") === -1)
                        throw err;
                    return _this.getIntrospectAttributesWithGet();
                });
            case 'segment-metadata-only':
                return this.getIntrospectAttributesWithSegmentMetadata(depth);
            case 'datasource-get':
                return this.getIntrospectAttributesWithGet();
            default:
                throw new Error('invalid introspectionStrategy');
        }
    };
    DruidExternal.prototype.groupAppliesByTimeFilterValue = function () {
        var _a;
        var _this = this;
        var _b = this, applies = _b.applies, sort = _b.sort;
        var groups = [];
        var constantApplies = [];
        var _loop_1 = function (apply) {
            if (apply.expression instanceof LiteralExpression) {
                constantApplies.push(apply);
                return "continue";
            }
            var applyFilterValue = null;
            var badCondition = false;
            var newApply = apply.changeExpression(apply.expression.substitute(function (ex) {
                if (ex instanceof OverlapExpression && _this.isTimeRef(ex.operand) && ex.expression.getLiteralValue()) {
                    var myValue = ex.expression.getLiteralValue();
                    if (applyFilterValue && !applyFilterValue.equals(myValue))
                        badCondition = true;
                    applyFilterValue = myValue;
                    return Expression.TRUE;
                }
                return null;
            }).simplify());
            if (badCondition || !applyFilterValue)
                return { value: null };
            var myGroup = groups.find(function (r) { return applyFilterValue.equals(r.filterValue); });
            var mySort = Boolean(sort && sort.expression instanceof RefExpression && newApply.name === sort.expression.name);
            if (myGroup) {
                myGroup.unfilteredApplies.push(newApply);
                if (mySort)
                    myGroup.hasSort = true;
            }
            else {
                groups.push({
                    filterValue: applyFilterValue,
                    unfilteredApplies: [newApply],
                    hasSort: mySort
                });
            }
        };
        for (var _i = 0, applies_2 = applies; _i < applies_2.length; _i++) {
            var apply = applies_2[_i];
            var state_1 = _loop_1(apply);
            if (typeof state_1 === "object")
                return state_1.value;
        }
        if (groups.length && constantApplies.length) {
            (_a = groups[0].unfilteredApplies).push.apply(_a, constantApplies);
        }
        return groups;
    };
    DruidExternal.prototype.getJoinDecompositionShortcut = function () {
        var _a;
        if (this.mode !== 'split')
            return null;
        var timeAttribute = this.timeAttribute;
        if (this.split.numSplits() !== 1)
            return null;
        var splitName = this.split.firstSplitName();
        var splitExpression = this.split.firstSplitExpression();
        var appliesByTimeFilterValue = this.groupAppliesByTimeFilterValue();
        if (!appliesByTimeFilterValue || appliesByTimeFilterValue.length !== 2)
            return null;
        var filterV0 = appliesByTimeFilterValue[0].filterValue;
        var filterV1 = appliesByTimeFilterValue[1].filterValue;
        if (!(filterV0 instanceof TimeRange && filterV1 instanceof TimeRange))
            return null;
        if (filterV0.start < filterV1.start)
            appliesByTimeFilterValue.reverse();
        if (splitExpression instanceof TimeBucketExpression && (!this.sort || this.sortOnLabel()) && !this.limit) {
            var fallbackExpression = splitExpression.operand;
            if (fallbackExpression instanceof FallbackExpression) {
                var timeShiftExpression = fallbackExpression.expression;
                if (timeShiftExpression instanceof TimeShiftExpression) {
                    var timeRef = timeShiftExpression.operand;
                    if (this.isTimeRef(timeRef)) {
                        var simpleSplit = this.split.changeSplits((_a = {}, _a[splitName] = splitExpression.changeOperand(timeRef), _a));
                        var external1Value = this.valueOf();
                        external1Value.filter = $(timeAttribute, 'TIME').overlap(appliesByTimeFilterValue[0].filterValue).and(external1Value.filter).simplify();
                        external1Value.split = simpleSplit;
                        external1Value.applies = appliesByTimeFilterValue[0].unfilteredApplies;
                        var external2Value = this.valueOf();
                        external2Value.filter = $(timeAttribute, 'TIME').overlap(appliesByTimeFilterValue[1].filterValue).and(external2Value.filter).simplify();
                        external2Value.split = simpleSplit;
                        external2Value.applies = appliesByTimeFilterValue[1].unfilteredApplies;
                        return {
                            external1: new DruidExternal(external1Value),
                            external2: new DruidExternal(external2Value),
                            timeShift: timeShiftExpression.changeOperand(Expression._)
                        };
                    }
                }
            }
        }
        if (appliesByTimeFilterValue[0].hasSort && this.limit && this.limit.value <= 1000) {
            var external1Value = this.valueOf();
            external1Value.filter = $(timeAttribute, 'TIME').overlap(appliesByTimeFilterValue[0].filterValue).and(external1Value.filter).simplify();
            external1Value.applies = appliesByTimeFilterValue[0].unfilteredApplies;
            var external2Value = this.valueOf();
            external2Value.filter = $(timeAttribute, 'TIME').overlap(appliesByTimeFilterValue[1].filterValue).and(external2Value.filter).simplify();
            external2Value.applies = appliesByTimeFilterValue[1].unfilteredApplies;
            external2Value.sort = external2Value.sort.changeExpression($(external2Value.applies[0].name));
            return {
                external1: new DruidExternal(external1Value),
                external2: new DruidExternal(external2Value),
                waterfallFilterExpression: external1Value.split
            };
        }
        return null;
    };
    DruidExternal.prototype.queryBasicValueStream = function (rawQueries, computeContext) {
        var decomposed = this.getJoinDecompositionShortcut();
        if (decomposed) {
            var waterfallFilterExpression_1 = decomposed.waterfallFilterExpression;
            if (waterfallFilterExpression_1) {
                return External.valuePromiseToStream(External.buildValueFromStream(decomposed.external1.queryBasicValueStream(rawQueries, computeContext)).then(function (pv1) {
                    var ds1 = pv1;
                    var ds1Filter = Expression.or(ds1.data.map(function (datum) { return waterfallFilterExpression_1.filterFromDatum(datum); }));
                    var ex2Value = decomposed.external2.valueOf();
                    ex2Value.filter = ex2Value.filter.and(ds1Filter);
                    var filteredExternal = new DruidExternal(ex2Value);
                    return External.buildValueFromStream(filteredExternal.queryBasicValueStream(rawQueries, computeContext)).then(function (pv2) {
                        return ds1.leftJoin(pv2);
                    });
                }));
            }
            else {
                var plywoodValue1Promise = External.buildValueFromStream(decomposed.external1.queryBasicValueStream(rawQueries, computeContext));
                var plywoodValue2Promise = External.buildValueFromStream(decomposed.external2.queryBasicValueStream(rawQueries, computeContext));
                return External.valuePromiseToStream(Promise.all([plywoodValue1Promise, plywoodValue2Promise]).then(function (_a) {
                    var pv1 = _a[0], pv2 = _a[1];
                    var ds1 = pv1;
                    var ds2 = pv2;
                    var timeShift = decomposed.timeShift;
                    if (timeShift && ds2.data.length) {
                        var timeLabel_1 = ds2.keys[0];
                        var timeShiftDuration_1 = timeShift.duration;
                        var timeShiftTimezone_1 = timeShift.timezone;
                        ds2 = ds2.applyFn(timeLabel_1, function (d) {
                            var tr = d[timeLabel_1];
                            var shiftedStart = timeShiftDuration_1.shift(tr.start, timeShiftTimezone_1, 1);
                            return new TimeRange({
                                start: shiftedStart,
                                end: shiftedStart,
                                bounds: '[]'
                            });
                        }, 'TIME_RANGE');
                    }
                    return ds1.fullJoin(ds2, function (a, b) { return a.start.valueOf() - b.start.valueOf(); });
                }));
            }
        }
        return _super.prototype.queryBasicValueStream.call(this, rawQueries, computeContext);
    };
    DruidExternal.engine = 'druid';
    DruidExternal.type = 'DATASET';
    DruidExternal.DUMMY_NAME = '!DUMMY';
    DruidExternal.TIME_ATTRIBUTE = '__time';
    DruidExternal.VALID_INTROSPECTION_STRATEGIES = ['segment-metadata-fallback', 'segment-metadata-only', 'datasource-get'];
    DruidExternal.DEFAULT_INTROSPECTION_STRATEGY = 'segment-metadata-fallback';
    DruidExternal.SELECT_INIT_LIMIT = 50;
    DruidExternal.SELECT_MAX_LIMIT = 10000;
    return DruidExternal;
}(External));
export { DruidExternal };
External.register(DruidExternal);
