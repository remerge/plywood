import * as hasOwnProp from 'has-own-prop';
import { NamedArray } from 'immutable-class';
import { $, AbsoluteExpression, AddExpression, CastExpression, ChainableUnaryExpression, ConcatExpression, CountDistinctExpression, CountExpression, CustomAggregateExpression, DivideExpression, Expression, FallbackExpression, FilterExpression, IndexOfExpression, LiteralExpression, MaxExpression, MinExpression, MultiplyExpression, PowerExpression, QuantileExpression, RefExpression, SubtractExpression, SumExpression, TransformCaseExpression } from '../../expressions';
import { External } from '../baseExternal';
import { DruidExpressionBuilder } from './druidExpressionBuilder';
import { DruidExtractionFnBuilder } from './druidExtractionFnBuilder';
import { DruidFilterBuilder } from './druidFilterBuilder';
var DruidAggregationBuilder = (function () {
    function DruidAggregationBuilder(options) {
        this.version = options.version;
        this.rawAttributes = options.rawAttributes;
        this.timeAttribute = options.timeAttribute;
        this.derivedAttributes = options.derivedAttributes;
        this.customAggregations = options.customAggregations;
        this.customTransforms = options.customTransforms;
        this.rollup = options.rollup;
        this.exactResultsOnly = options.exactResultsOnly;
        this.allowEternity = options.allowEternity;
    }
    DruidAggregationBuilder.addOptionsToAggregation = function (aggregation, expression) {
        var options = expression.options;
        if (options && options.csum) {
            aggregation._csum = true;
        }
    };
    DruidAggregationBuilder.prototype.makeAggregationsAndPostAggregations = function (applies) {
        var _this = this;
        var _a = External.segregationAggregateApplies(applies.map(function (apply) {
            var expression = apply.expression;
            expression = _this.switchToRollupCount(_this.inlineDerivedAttributesInAggregate(expression).decomposeAverage()).distribute();
            return apply.changeExpression(expression);
        })), aggregateApplies = _a.aggregateApplies, postAggregateApplies = _a.postAggregateApplies;
        var aggregations = [];
        var postAggregations = [];
        for (var _i = 0, aggregateApplies_1 = aggregateApplies; _i < aggregateApplies_1.length; _i++) {
            var aggregateApply = aggregateApplies_1[_i];
            this.applyToAggregation(aggregateApply, aggregations, postAggregations);
        }
        for (var _b = 0, postAggregateApplies_1 = postAggregateApplies; _b < postAggregateApplies_1.length; _b++) {
            var postAggregateApply = postAggregateApplies_1[_b];
            this.applyToPostAggregation(postAggregateApply, aggregations, postAggregations);
        }
        return {
            aggregations: aggregations,
            postAggregations: postAggregations
        };
    };
    DruidAggregationBuilder.prototype.applyToAggregation = function (action, aggregations, postAggregations) {
        var name = action.name, expression = action.expression;
        this.expressionToAggregation(name, expression, aggregations, postAggregations);
    };
    DruidAggregationBuilder.prototype.applyToPostAggregation = function (apply, aggregations, postAggregations) {
        var postAgg = this.expressionToPostAggregation(apply.expression, aggregations, postAggregations);
        postAgg.name = apply.name;
        postAggregations.push(postAgg);
    };
    DruidAggregationBuilder.prototype.filterAggregateIfNeeded = function (datasetExpression, aggregator) {
        if (datasetExpression instanceof FilterExpression) {
            return {
                type: "filtered",
                name: aggregator.name,
                filter: new DruidFilterBuilder(this).timelessFilterToFilter(datasetExpression.expression),
                aggregator: aggregator
            };
        }
        else if (datasetExpression instanceof RefExpression) {
            return aggregator;
        }
        else {
            throw new Error("could not construct aggregate on " + datasetExpression);
        }
    };
    DruidAggregationBuilder.prototype.expressionToAggregation = function (name, expression, aggregations, postAggregations) {
        var initAggregationsLength = aggregations.length;
        if (expression instanceof CountExpression) {
            aggregations.push(this.countToAggregation(name, expression));
        }
        else if (expression instanceof SumExpression || expression instanceof MinExpression || expression instanceof MaxExpression) {
            aggregations.push(this.sumMinMaxToAggregation(name, expression));
        }
        else if (expression instanceof CountDistinctExpression) {
            aggregations.push(this.countDistinctToAggregation(name, expression, postAggregations));
        }
        else if (expression instanceof QuantileExpression) {
            aggregations.push(this.quantileToAggregation(name, expression, postAggregations));
        }
        else if (expression instanceof CustomAggregateExpression) {
            this.customAggregateToAggregation(name, expression, aggregations, postAggregations);
        }
        else {
            throw new Error("unsupported aggregate action " + expression + " (as " + name + ")");
        }
        var finalAggregationsLength = aggregations.length;
        for (var i = initAggregationsLength; i < finalAggregationsLength; i++) {
            DruidAggregationBuilder.addOptionsToAggregation(aggregations[i], expression);
        }
    };
    DruidAggregationBuilder.prototype.countToAggregation = function (name, expression) {
        return this.filterAggregateIfNeeded(expression.operand, {
            name: name,
            type: 'count'
        });
    };
    DruidAggregationBuilder.prototype.sumMinMaxToAggregation = function (name, expression) {
        var op = expression.op;
        var opCap = op[0].toUpperCase() + op.substr(1);
        var aggregation;
        var aggregateExpression = expression.expression;
        if (aggregateExpression instanceof RefExpression) {
            var refName = aggregateExpression.name;
            var attributeInfo = this.getAttributesInfo(refName);
            if (attributeInfo.nativeType === 'STRING') {
                try {
                    aggregation = {
                        name: name,
                        type: 'double' + opCap,
                        expression: new DruidExpressionBuilder(this).expressionToDruidExpression(aggregateExpression.cast('NUMBER'))
                    };
                }
                catch (_a) {
                    aggregation = this.makeJavaScriptAggregation(name, expression);
                }
            }
            else {
                aggregation = {
                    name: name,
                    type: (attributeInfo.nativeType === 'LONG' ? 'long' : 'double') + opCap,
                    fieldName: refName
                };
            }
        }
        else {
            try {
                aggregation = {
                    name: name,
                    type: 'double' + opCap,
                    expression: new DruidExpressionBuilder(this).expressionToDruidExpression(aggregateExpression)
                };
            }
            catch (_b) {
                aggregation = this.makeJavaScriptAggregation(name, expression);
            }
        }
        return this.filterAggregateIfNeeded(expression.operand, aggregation);
    };
    DruidAggregationBuilder.prototype.getCardinalityExpressions = function (expression) {
        var _this = this;
        if (expression instanceof LiteralExpression) {
            return [];
        }
        else if (expression instanceof CastExpression) {
            return [expression.operand];
        }
        else if (expression instanceof ConcatExpression) {
            var subEx = expression.getExpressionList().map(function (ex) { return _this.getCardinalityExpressions(ex); });
            return [].concat.apply([], subEx);
        }
        else if (expression.getFreeReferences().length === 1) {
            return [expression];
        }
        else {
            throw new Error("can not convert " + expression + " to cardinality expressions");
        }
    };
    DruidAggregationBuilder.prototype.countDistinctToAggregation = function (name, expression, postAggregations) {
        var _this = this;
        if (this.exactResultsOnly) {
            throw new Error("approximate query not allowed");
        }
        var aggregation;
        var attribute = expression.expression;
        var forceFinalize = expression.getOptions().forceFinalize;
        if (attribute instanceof RefExpression) {
            var attributeName = attribute.name;
            var attributeInfo = this.getAttributesInfo(attributeName);
            var tempName = void 0;
            switch (attributeInfo.nativeType) {
                case 'hyperUnique':
                    tempName = '!Hyper_' + name;
                    aggregation = {
                        name: forceFinalize ? tempName : name,
                        type: "hyperUnique",
                        fieldName: attributeName
                    };
                    if (!this.versionBefore('0.10.1'))
                        aggregation.round = true;
                    if (forceFinalize) {
                        postAggregations.push({
                            type: 'finalizingFieldAccess',
                            name: name,
                            fieldName: tempName
                        });
                    }
                    break;
                case 'thetaSketch':
                    tempName = '!Theta_' + name;
                    postAggregations.push({
                        type: "thetaSketchEstimate",
                        name: name,
                        field: { type: 'fieldAccess', fieldName: tempName }
                    });
                    aggregation = {
                        name: tempName,
                        type: "thetaSketch",
                        fieldName: attributeName
                    };
                    break;
                case 'HLLSketch':
                    tempName = '!HLLSketch_' + name;
                    aggregation = {
                        name: forceFinalize ? tempName : name,
                        type: "HLLSketchMerge",
                        fieldName: attributeName
                    };
                    if (forceFinalize) {
                        postAggregations.push({
                            type: 'finalizingFieldAccess',
                            name: name,
                            fieldName: tempName
                        });
                    }
                    break;
                default:
                    tempName = '!Card_' + name;
                    aggregation = {
                        name: forceFinalize ? tempName : name,
                        type: "cardinality",
                        fields: [attributeName]
                    };
                    if (!this.versionBefore('0.10.1'))
                        aggregation.round = true;
                    if (forceFinalize) {
                        postAggregations.push({
                            type: 'finalizingFieldAccess',
                            name: name,
                            fieldName: tempName
                        });
                    }
                    break;
            }
        }
        else {
            var cardinalityExpressions = this.getCardinalityExpressions(attribute);
            var druidExtractionFnBuilder_1;
            aggregation = {
                name: name,
                type: "cardinality",
                fields: cardinalityExpressions.map(function (cardinalityExpression) {
                    if (cardinalityExpression instanceof RefExpression)
                        return cardinalityExpression.name;
                    if (!druidExtractionFnBuilder_1)
                        druidExtractionFnBuilder_1 = new DruidExtractionFnBuilder(_this, true);
                    return {
                        type: "extraction",
                        dimension: cardinalityExpression.getFreeReferences()[0],
                        extractionFn: druidExtractionFnBuilder_1.expressionToExtractionFn(cardinalityExpression)
                    };
                })
            };
            if (!this.versionBefore('0.10.1'))
                aggregation.round = true;
            if (cardinalityExpressions.length > 1)
                aggregation.byRow = true;
        }
        return this.filterAggregateIfNeeded(expression.operand, aggregation);
    };
    DruidAggregationBuilder.prototype.customAggregateToAggregation = function (name, expression, aggregations, postAggregations) {
        var _this = this;
        var customAggregationName = expression.custom;
        var customAggregation = this.customAggregations[customAggregationName];
        if (!customAggregation)
            throw new Error("could not find '" + customAggregationName + "'");
        var nonce = String(Math.random()).substr(2);
        var aggregationObjs = (Array.isArray(customAggregation.aggregations) ?
            customAggregation.aggregations :
            (customAggregation.aggregation ? [customAggregation.aggregation] : [])).map(function (a) {
            try {
                return JSON.parse(JSON.stringify(a).replace(/\{\{random\}\}/g, nonce));
            }
            catch (e) {
                throw new Error("must have JSON custom aggregation '" + customAggregationName + "'");
            }
        });
        var postAggregationObj = customAggregation.postAggregation;
        if (postAggregationObj) {
            try {
                postAggregationObj = JSON.parse(JSON.stringify(postAggregationObj).replace(/\{\{random\}\}/g, nonce));
            }
            catch (e) {
                throw new Error("must have JSON custom post aggregation '" + customAggregationName + "'");
            }
            postAggregationObj.name = name;
            postAggregations.push(postAggregationObj);
        }
        else {
            if (!aggregationObjs.length)
                throw new Error("must have an aggregation or postAggregation in custom aggregation '" + customAggregationName + "'");
            aggregationObjs[0].name = name;
        }
        aggregationObjs = aggregationObjs.map(function (a) { return _this.filterAggregateIfNeeded(expression.operand, a); });
        aggregations.push.apply(aggregations, aggregationObjs);
    };
    DruidAggregationBuilder.prototype.quantileToAggregation = function (name, expression, postAggregations) {
        if (this.exactResultsOnly) {
            throw new Error("approximate query not allowed");
        }
        var attribute = expression.expression;
        var attributeName;
        if (attribute instanceof RefExpression) {
            attributeName = attribute.name;
        }
        else {
            throw new Error("can not compute quantile on derived attribute: " + attribute);
        }
        var tuning = Expression.parseTuning(expression.tuning);
        var addTuningsToAggregation = function (aggregation, tuningKeys) {
            for (var _i = 0, tuningKeys_1 = tuningKeys; _i < tuningKeys_1.length; _i++) {
                var k = tuningKeys_1[_i];
                if (!isNaN(tuning[k])) {
                    aggregation[k] = Number(tuning[k]);
                }
            }
        };
        var attributeInfo = this.getAttributesInfo(attributeName);
        var aggregation;
        var tempName;
        switch (attributeInfo.nativeType) {
            case 'approximateHistogram':
                tempName = "!H_" + name;
                aggregation = {
                    name: tempName,
                    type: 'approxHistogramFold',
                    fieldName: attributeName
                };
                addTuningsToAggregation(aggregation, DruidAggregationBuilder.APPROX_HISTOGRAM_TUNINGS);
                postAggregations.push({
                    name: name,
                    type: "quantile",
                    fieldName: tempName,
                    probability: expression.value
                });
                break;
            case 'quantilesDoublesSketch':
                tempName = "!QD_" + name;
                aggregation = {
                    name: tempName,
                    type: 'quantilesDoublesSketch',
                    fieldName: attributeName
                };
                addTuningsToAggregation(aggregation, DruidAggregationBuilder.QUANTILES_DOUBLES_TUNINGS);
                postAggregations.push({
                    name: name,
                    type: "quantilesDoublesSketchToQuantile",
                    field: {
                        type: "fieldAccess",
                        fieldName: tempName
                    },
                    fraction: expression.value
                });
                break;
            default:
                if (Number(tuning['v']) === 2) {
                    tempName = "!QD_" + name;
                    aggregation = {
                        name: tempName,
                        type: 'quantilesDoublesSketch',
                        fieldName: attributeName
                    };
                    addTuningsToAggregation(aggregation, DruidAggregationBuilder.QUANTILES_DOUBLES_TUNINGS);
                    postAggregations.push({
                        name: name,
                        type: "quantilesDoublesSketchToQuantile",
                        field: {
                            type: "fieldAccess",
                            fieldName: tempName
                        },
                        fraction: expression.value
                    });
                }
                else {
                    tempName = "!H_" + name;
                    aggregation = {
                        name: tempName,
                        type: 'approxHistogram',
                        fieldName: attributeName
                    };
                    addTuningsToAggregation(aggregation, DruidAggregationBuilder.APPROX_HISTOGRAM_TUNINGS);
                    postAggregations.push({
                        name: name,
                        type: "quantile",
                        fieldName: tempName,
                        probability: expression.value
                    });
                }
                break;
        }
        return this.filterAggregateIfNeeded(expression.operand, aggregation);
    };
    DruidAggregationBuilder.prototype.makeJavaScriptAggregation = function (name, aggregate) {
        if (aggregate instanceof ChainableUnaryExpression) {
            var aggregateType = aggregate.op;
            var aggregateExpression = aggregate.expression;
            var aggregateFunction = DruidAggregationBuilder.AGGREGATE_TO_FUNCTION[aggregateType];
            if (!aggregateFunction)
                throw new Error("Can not convert " + aggregateType + " to JS");
            var zero = DruidAggregationBuilder.AGGREGATE_TO_ZERO[aggregateType];
            var fieldNames = aggregateExpression.getFreeReferences();
            var simpleFieldNames = fieldNames.map(RefExpression.toJavaScriptSafeName);
            return {
                name: name,
                type: "javascript",
                fieldNames: fieldNames,
                fnAggregate: "function($$," + simpleFieldNames.join(',') + ") { return " + aggregateFunction('$$', aggregateExpression.getJS(null)) + "; }",
                fnCombine: "function(a,b) { return " + aggregateFunction('a', 'b') + "; }",
                fnReset: "function() { return " + zero + "; }"
            };
        }
        else {
            throw new Error("Can not convert " + aggregate + " to JS aggregate");
        }
    };
    DruidAggregationBuilder.prototype.getAccessTypeForAggregation = function (aggregationType) {
        if (aggregationType === 'hyperUnique' || aggregationType === 'cardinality')
            return 'hyperUniqueCardinality';
        var customAggregations = this.customAggregations;
        for (var customName in customAggregations) {
            if (!hasOwnProp(customAggregations, customName))
                continue;
            var customAggregation = customAggregations[customName];
            if ((customAggregation.aggregation && customAggregation.aggregation.type === aggregationType) ||
                (Array.isArray(customAggregation.aggregations) && customAggregation.aggregations.find(function (a) { return a.type === aggregationType; }))) {
                return customAggregation.accessType || 'fieldAccess';
            }
        }
        return 'fieldAccess';
    };
    DruidAggregationBuilder.prototype.getAccessType = function (aggregations, aggregationName) {
        for (var _i = 0, aggregations_1 = aggregations; _i < aggregations_1.length; _i++) {
            var aggregation = aggregations_1[_i];
            if (aggregation.name === aggregationName) {
                var aggregationType = aggregation.type;
                if (aggregationType === 'filtered')
                    aggregationType = aggregation.aggregator.type;
                return this.getAccessTypeForAggregation(aggregationType);
            }
        }
        return 'fieldAccess';
    };
    DruidAggregationBuilder.prototype.expressionToPostAggregation = function (ex, aggregations, postAggregations) {
        var druidExpression = new DruidExpressionBuilder(this).expressionToDruidExpression(ex);
        if (!druidExpression) {
            return this.expressionToLegacyPostAggregation(ex, aggregations, postAggregations);
        }
        return {
            type: "expression",
            expression: druidExpression
        };
    };
    DruidAggregationBuilder.prototype.expressionToLegacyPostAggregation = function (ex, aggregations, postAggregations) {
        var _this = this;
        if (ex instanceof RefExpression) {
            var refName = ex.name;
            return {
                type: this.getAccessType(aggregations, refName),
                fieldName: refName
            };
        }
        else if (ex instanceof LiteralExpression) {
            if (ex.type !== 'NUMBER')
                throw new Error("must be a NUMBER type");
            return {
                type: 'constant',
                value: ex.value
            };
        }
        else if (ex instanceof AbsoluteExpression ||
            ex instanceof PowerExpression ||
            ex instanceof FallbackExpression ||
            ex instanceof CastExpression ||
            ex instanceof IndexOfExpression ||
            ex instanceof TransformCaseExpression) {
            var fieldNameRefs = ex.getFreeReferences();
            var fieldNames = fieldNameRefs.map(function (fieldNameRef) {
                var accessType = _this.getAccessType(aggregations, fieldNameRef);
                if (accessType === 'fieldAccess')
                    return fieldNameRef;
                var fieldNameRefTemp = '!F_' + fieldNameRef;
                postAggregations.push({
                    name: fieldNameRefTemp,
                    type: accessType,
                    fieldName: fieldNameRef
                });
                return fieldNameRefTemp;
            });
            return {
                name: 'dummy',
                type: 'javascript',
                fieldNames: fieldNames,
                'function': "function(" + fieldNameRefs.map(RefExpression.toJavaScriptSafeName) + ") { return " + ex.getJS(null) + "; }"
            };
        }
        else if (ex instanceof AddExpression) {
            return {
                type: 'arithmetic',
                fn: '+',
                fields: ex.getExpressionList().map(function (e) { return _this.expressionToPostAggregation(e, aggregations, postAggregations); })
            };
        }
        else if (ex instanceof SubtractExpression) {
            return {
                type: 'arithmetic',
                fn: '-',
                fields: ex.getExpressionList().map(function (e) { return _this.expressionToPostAggregation(e, aggregations, postAggregations); })
            };
        }
        else if (ex instanceof MultiplyExpression) {
            return {
                type: 'arithmetic',
                fn: '*',
                fields: ex.getExpressionList().map(function (e) { return _this.expressionToPostAggregation(e, aggregations, postAggregations); })
            };
        }
        else if (ex instanceof DivideExpression) {
            return {
                type: 'arithmetic',
                fn: '/',
                fields: ex.getExpressionList().map(function (e) { return _this.expressionToPostAggregation(e, aggregations, postAggregations); })
            };
        }
        else {
            throw new Error("can not convert expression to post agg: " + ex);
        }
    };
    DruidAggregationBuilder.prototype.switchToRollupCount = function (expression) {
        var _this = this;
        if (!this.rollup)
            return expression;
        var countRef = null;
        return expression.substitute(function (ex) {
            if (ex instanceof CountExpression) {
                if (!countRef)
                    countRef = $(_this.getRollupCountName(), 'NUMBER');
                return ex.operand.sum(countRef);
            }
            return null;
        });
    };
    DruidAggregationBuilder.prototype.getRollupCountName = function () {
        var rawAttributes = this.rawAttributes;
        for (var _i = 0, rawAttributes_1 = rawAttributes; _i < rawAttributes_1.length; _i++) {
            var attribute = rawAttributes_1[_i];
            var maker = attribute.maker;
            if (maker && maker.op === 'count')
                return attribute.name;
        }
        throw new Error("could not find rollup count");
    };
    DruidAggregationBuilder.prototype.inlineDerivedAttributes = function (expression) {
        var derivedAttributes = this.derivedAttributes;
        return expression.substitute(function (refEx) {
            if (refEx instanceof RefExpression) {
                return derivedAttributes[refEx.name] || null;
            }
            else {
                return null;
            }
        });
    };
    DruidAggregationBuilder.prototype.inlineDerivedAttributesInAggregate = function (expression) {
        var _this = this;
        return expression.substitute(function (ex) {
            if (ex.isAggregate()) {
                return _this.inlineDerivedAttributes(ex);
            }
            return null;
        });
    };
    DruidAggregationBuilder.prototype.getAttributesInfo = function (attributeName) {
        return NamedArray.get(this.rawAttributes, attributeName);
    };
    DruidAggregationBuilder.prototype.versionBefore = function (neededVersion) {
        var version = this.version;
        return version && External.versionLessThan(version, neededVersion);
    };
    DruidAggregationBuilder.AGGREGATE_TO_FUNCTION = {
        sum: function (a, b) { return a + "+" + b; },
        min: function (a, b) { return "Math.min(" + a + "," + b + ")"; },
        max: function (a, b) { return "Math.max(" + a + "," + b + ")"; }
    };
    DruidAggregationBuilder.AGGREGATE_TO_ZERO = {
        sum: "0",
        min: "Infinity",
        max: "-Infinity"
    };
    DruidAggregationBuilder.APPROX_HISTOGRAM_TUNINGS = [
        "resolution",
        "numBuckets",
        "lowerLimit",
        "upperLimit"
    ];
    DruidAggregationBuilder.QUANTILES_DOUBLES_TUNINGS = [
        "k"
    ];
    return DruidAggregationBuilder;
}());
export { DruidAggregationBuilder };
