import * as tslib_1 from "tslib";
import { Duration, parseISODate, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { isImmutableClass, generalLookupsEqual } from 'immutable-class';
import { PassThrough } from 'readable-stream';
import { failIfIntrospectNeededInDatum, getFullTypeFromDatum, introspectDatum } from '../datatypes/common';
import { Dataset, fillExpressionExternalAlteration, sizeOfDatasetExternalAlterations, NumberRange, Range, Set, StringRange, TimeRange } from '../datatypes/index';
import { iteratorFactory } from '../datatypes/valueStream';
import { External } from '../external/baseExternal';
import { promiseWhile } from '../helper/promiseWhile';
import { deduplicateSort, pipeWithError, repeat, shallowCopy } from '../helper/utils';
import { AbsoluteExpression } from './absoluteExpression';
import { AddExpression } from './addExpression';
import { AndExpression } from './andExpression';
import { ApplyExpression } from './applyExpression';
import { AverageExpression } from './averageExpression';
import { CardinalityExpression } from './cardinalityExpression';
import { CastExpression } from './castExpression';
import { CollectExpression } from './collectExpression';
import { ConcatExpression } from './concatExpression';
import { ContainsExpression } from './containsExpression';
import { CountDistinctExpression } from './countDistinctExpression';
import { CountExpression } from './countExpression';
import { CustomAggregateExpression } from './customAggregateExpression';
import { CustomTransformExpression } from './customTransformExpression';
import { DivideExpression } from './divideExpression';
import { ExternalExpression } from './externalExpression';
import { ExtractExpression } from './extractExpression';
import { FallbackExpression } from './fallbackExpression';
import { FilterExpression } from './filterExpression';
import { GreaterThanExpression } from './greaterThanExpression';
import { GreaterThanOrEqualExpression } from './greaterThanOrEqualExpression';
import { IndexOfExpression } from './indexOfExpression';
import { InExpression } from './inExpression';
import { IsExpression } from './isExpression';
import { JoinExpression } from './joinExpression';
import { LengthExpression } from './lengthExpression';
import { LessThanExpression } from './lessThanExpression';
import { LessThanOrEqualExpression } from './lessThanOrEqualExpression';
import { LimitExpression } from './limitExpression';
import { LiteralExpression } from './literalExpression';
import { LookupExpression } from './lookupExpression';
import { MatchExpression } from './matchExpression';
import { MaxExpression } from './maxExpression';
import { MinExpression } from './minExpression';
import { MultiplyExpression } from './multiplyExpression';
import { NotExpression } from './notExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { OrExpression } from './orExpression';
import { OverlapExpression } from './overlapExpression';
import { PowerExpression } from './powerExpression';
import { LogExpression } from './logExpression';
import { QuantileExpression } from './quantileExpression';
import { RefExpression } from './refExpression';
import { SelectExpression } from './selectExpression';
import { SortExpression } from './sortExpression';
import { SplitExpression } from './splitExpression';
import { SubstrExpression } from './substrExpression';
import { SubtractExpression } from './subtractExpression';
import { SumExpression } from './sumExpression';
import { ThenExpression } from './thenExpression';
import { TimeBucketExpression } from './timeBucketExpression';
import { TimeFloorExpression } from './timeFloorExpression';
import { TimePartExpression } from './timePartExpression';
import { TimeRangeExpression } from './timeRangeExpression';
import { TimeShiftExpression } from './timeShiftExpression';
import { TransformCaseExpression } from './transformCaseExpression';
function fillExpressionExternalAlterationAsync(alteration, filler) {
    var tasks = [];
    fillExpressionExternalAlteration(alteration, function (external, terminal) {
        tasks.push(filler(external, terminal));
        return null;
    });
    return Promise.all(tasks).then(function (results) {
        var i = 0;
        fillExpressionExternalAlteration(alteration, function () {
            var res = results[i];
            i++;
            return res;
        });
        return alteration;
    });
}
function runtimeAbstract() {
    return new Error('must be implemented');
}
function getDataName(ex) {
    if (ex instanceof RefExpression) {
        return ex.name;
    }
    else if (ex instanceof ChainableExpression) {
        return getDataName(ex.operand);
    }
    else {
        return null;
    }
}
function getValue(param) {
    if (param instanceof LiteralExpression)
        return param.value;
    return param;
}
function getString(param) {
    if (typeof param === 'string')
        return param;
    if (param instanceof LiteralExpression && param.type === 'STRING') {
        return param.value;
    }
    if (param instanceof RefExpression && param.nest === 0) {
        return param.name;
    }
    throw new Error('could not extract a string out of ' + String(param));
}
function getNumber(param) {
    if (typeof param === 'number')
        return param;
    if (param instanceof LiteralExpression && param.type === 'NUMBER') {
        return param.value;
    }
    throw new Error('could not extract a number out of ' + String(param));
}
export function ply(dataset) {
    if (!dataset) {
        dataset = new Dataset({
            keys: [],
            data: [{}]
        });
    }
    return r(dataset);
}
export function $(name, nest, type) {
    if (typeof name !== 'string')
        throw new TypeError('$() argument must be a string');
    if (typeof nest === 'string') {
        type = nest;
        nest = 0;
    }
    return new RefExpression({
        name: name,
        nest: nest != null ? nest : 0,
        type: type
    });
}
export function i$(name, nest, type) {
    if (typeof name !== 'string')
        throw new TypeError('$() argument must be a string');
    if (typeof nest === 'string') {
        type = nest;
        nest = 0;
    }
    return new RefExpression({
        name: name,
        nest: nest != null ? nest : 0,
        type: type,
        ignoreCase: true
    });
}
export function r(value) {
    if (value instanceof External)
        throw new TypeError('r() can not accept externals');
    if (Array.isArray(value))
        value = Set.fromJS(value);
    return LiteralExpression.fromJS({ op: 'literal', value: value });
}
export function toJS(thing) {
    return (thing && typeof thing.toJS === 'function') ? thing.toJS() : thing;
}
function chainVia(op, expressions, zero) {
    var n = expressions.length;
    if (!n)
        return zero;
    var acc = expressions[0];
    if (!(acc instanceof Expression))
        acc = Expression.fromJSLoose(acc);
    for (var i = 1; i < n; i++)
        acc = acc[op](expressions[i]);
    return acc;
}
var Expression = (function () {
    function Expression(parameters, dummy) {
        if (dummy === void 0) { dummy = null; }
        this.op = parameters.op;
        if (dummy !== dummyObject) {
            throw new TypeError("can not call `new Expression` directly use Expression.fromJS instead");
        }
        if (parameters.simple)
            this.simple = true;
        if (parameters.options)
            this.options = parameters.options;
    }
    Expression.isExpression = function (candidate) {
        return candidate instanceof Expression;
    };
    Expression.expressionLookupFromJS = function (expressionJSs) {
        var expressions = Object.create(null);
        for (var name_1 in expressionJSs) {
            if (!hasOwnProp(expressionJSs, name_1))
                continue;
            expressions[name_1] = Expression.fromJSLoose(expressionJSs[name_1]);
        }
        return expressions;
    };
    Expression.expressionLookupToJS = function (expressions) {
        var expressionsJSs = {};
        for (var name_2 in expressions) {
            if (!hasOwnProp(expressions, name_2))
                continue;
            expressionsJSs[name_2] = expressions[name_2].toJS();
        }
        return expressionsJSs;
    };
    Expression.parse = function (str, timezone) {
        if (str[0] === '{' && str[str.length - 1] === '}') {
            return Expression.fromJS(JSON.parse(str));
        }
        var original = Expression.defaultParserTimezone;
        if (timezone)
            Expression.defaultParserTimezone = timezone;
        try {
            return Expression.expressionParser.parse(str);
        }
        catch (e) {
            throw new Error("Expression parse error: " + e.message + " on '" + str + "'");
        }
        finally {
            Expression.defaultParserTimezone = original;
        }
    };
    Expression.parseSQL = function (str, timezone) {
        var original = Expression.defaultParserTimezone;
        if (timezone)
            Expression.defaultParserTimezone = timezone;
        try {
            return Expression.plyqlParser.parse(str);
        }
        catch (e) {
            throw new Error("SQL parse error: " + e.message + " on '" + str + "'");
        }
        finally {
            Expression.defaultParserTimezone = original;
        }
    };
    Expression.fromJSLoose = function (param) {
        var expressionJS;
        switch (typeof param) {
            case 'undefined':
                throw new Error('must have an expression');
            case 'object':
                if (param === null) {
                    return Expression.NULL;
                }
                else if (param instanceof Expression) {
                    return param;
                }
                else if (isImmutableClass(param)) {
                    if (param.constructor.type) {
                        expressionJS = { op: 'literal', value: param };
                    }
                    else {
                        throw new Error("unknown object");
                    }
                }
                else if (param.op) {
                    expressionJS = param;
                }
                else if (param.toISOString) {
                    expressionJS = { op: 'literal', value: new Date(param) };
                }
                else if (Array.isArray(param)) {
                    expressionJS = { op: 'literal', value: Set.fromJS(param) };
                }
                else if (hasOwnProp(param, 'start') && hasOwnProp(param, 'end')) {
                    expressionJS = { op: 'literal', value: Range.fromJS(param) };
                }
                else {
                    throw new Error('unknown parameter');
                }
                break;
            case 'number':
            case 'boolean':
                expressionJS = { op: 'literal', value: param };
                break;
            case 'string':
                return Expression.parse(param);
            default:
                throw new Error("unrecognizable expression");
        }
        return Expression.fromJS(expressionJS);
    };
    Expression.jsNullSafetyUnary = function (inputJS, ifNotNull) {
        return "(_=" + inputJS + ",(_==null?null:" + ifNotNull('_') + "))";
    };
    Expression.jsNullSafetyBinary = function (lhs, rhs, combine, lhsCantBeNull, rhsCantBeNull) {
        if (lhsCantBeNull) {
            if (rhsCantBeNull) {
                return "(" + combine(lhs, rhs) + ")";
            }
            else {
                return "(_=" + rhs + ",(_==null)?null:(" + combine(lhs, '_') + "))";
            }
        }
        else {
            if (rhsCantBeNull) {
                return "(_=" + lhs + ",(_==null)?null:(" + combine('_', rhs) + "))";
            }
            else {
                return "(_=" + rhs + ",_2=" + lhs + ",(_==null||_2==null)?null:(" + combine('_', '_2') + ")";
            }
        }
    };
    Expression.parseTuning = function (tuning) {
        if (typeof tuning !== 'string')
            return {};
        var parts = tuning.split(',');
        var parsed = {};
        for (var _i = 0, parts_1 = parts; _i < parts_1.length; _i++) {
            var part = parts_1[_i];
            var subParts = part.split('=');
            if (subParts.length !== 2)
                throw new Error("can not parse tuning '" + tuning + "'");
            parsed[subParts[0]] = subParts[1];
        }
        return parsed;
    };
    Expression.safeString = function (str) {
        return /^[a-z]\w+$/i.test(str) ? str : JSON.stringify(str);
    };
    Expression.and = function (expressions) {
        return chainVia('and', expressions, Expression.TRUE);
    };
    Expression.or = function (expressions) {
        return chainVia('or', expressions, Expression.FALSE);
    };
    Expression.add = function (expressions) {
        return chainVia('add', expressions, Expression.ZERO);
    };
    Expression.subtract = function (expressions) {
        return chainVia('subtract', expressions, Expression.ZERO);
    };
    Expression.multiply = function (expressions) {
        return chainVia('multiply', expressions, Expression.ONE);
    };
    Expression.power = function (expressions) {
        return chainVia('power', expressions, Expression.ZERO);
    };
    Expression.concat = function (expressions) {
        return chainVia('concat', expressions, Expression.EMPTY_STRING);
    };
    Expression.register = function (ex) {
        var op = ex.op.replace(/^\w/, function (s) { return s.toLowerCase(); });
        Expression.classMap[op] = ex;
    };
    Expression.getConstructorFor = function (op) {
        var ClassFn = Expression.classMap[op];
        if (!ClassFn)
            throw new Error("unsupported expression op '" + op + "'");
        return ClassFn;
    };
    Expression.applyMixins = function (derivedCtor, baseCtors) {
        baseCtors.forEach(function (baseCtor) {
            Object.getOwnPropertyNames(baseCtor.prototype).forEach(function (name) {
                derivedCtor.prototype[name] = baseCtor.prototype[name];
            });
        });
    };
    Expression.jsToValue = function (js) {
        return {
            op: js.op,
            type: js.type,
            options: js.options
        };
    };
    Expression.fromJS = function (expressionJS) {
        if (!expressionJS)
            throw new Error('must have expressionJS');
        if (!hasOwnProp(expressionJS, "op")) {
            if (hasOwnProp(expressionJS, "action")) {
                expressionJS = shallowCopy(expressionJS);
                expressionJS.op = expressionJS.action;
                delete expressionJS.action;
                expressionJS.operand = { op: 'ref', name: '_' };
            }
            else {
                throw new Error("op must be defined");
            }
        }
        if (expressionJS.op === 'custom') {
            expressionJS = shallowCopy(expressionJS);
            expressionJS.op = 'customAggregate';
        }
        var op = expressionJS.op;
        if (typeof op !== "string") {
            throw new Error("op must be a string");
        }
        if (op === 'chain') {
            var actions = expressionJS.actions || [expressionJS.action];
            return Expression.fromJS(expressionJS.expression).performActions(actions.map(Expression.fromJS));
        }
        var ClassFn = Expression.getConstructorFor(op);
        return ClassFn.fromJS(expressionJS);
    };
    Expression.fromValue = function (parameters) {
        var op = parameters.op;
        var ClassFn = Expression.getConstructorFor(op);
        return new ClassFn(parameters);
    };
    Expression.prototype._ensureOp = function (op) {
        if (!this.op) {
            this.op = op;
            return;
        }
        if (this.op !== op) {
            throw new TypeError("incorrect expression op '" + this.op + "' (needs to be: '" + op + "')");
        }
    };
    Expression.prototype.valueOf = function () {
        var value = { op: this.op };
        if (this.simple)
            value.simple = true;
        if (this.options)
            value.options = this.options;
        return value;
    };
    Expression.prototype.toJS = function () {
        var js = { op: this.op };
        if (this.options)
            js.options = this.options;
        return js;
    };
    Expression.prototype.toJSON = function () {
        return this.toJS();
    };
    Expression.prototype.equals = function (other) {
        return other instanceof Expression &&
            this.op === other.op &&
            this.type === other.type &&
            generalLookupsEqual(this.options, other.options);
    };
    Expression.prototype.canHaveType = function (wantedType) {
        var type = this.type;
        if (!type || type === 'NULL')
            return true;
        if (wantedType === 'SET') {
            return Set.isSetType(type);
        }
        else {
            return type === wantedType;
        }
    };
    Expression.prototype.expressionCount = function () {
        return 1;
    };
    Expression.prototype.isOp = function (op) {
        return this.op === op;
    };
    Expression.prototype.markSimple = function () {
        if (this.simple)
            return this;
        var value = this.valueOf();
        value.simple = true;
        return Expression.fromValue(value);
    };
    Expression.prototype.containsOp = function (op) {
        return this.some(function (ex) { return ex.isOp(op) || null; });
    };
    Expression.prototype.hasExternal = function () {
        return this.some(function (ex) {
            if (ex instanceof ExternalExpression)
                return true;
            return null;
        });
    };
    Expression.prototype.getBaseExternals = function () {
        var externals = [];
        this.forEach(function (ex) {
            if (ex instanceof ExternalExpression)
                externals.push(ex.external.getBase());
        });
        return External.deduplicateExternals(externals);
    };
    Expression.prototype.getRawExternals = function () {
        var externals = [];
        this.forEach(function (ex) {
            if (ex instanceof ExternalExpression)
                externals.push(ex.external.getRaw());
        });
        return External.deduplicateExternals(externals);
    };
    Expression.prototype.getReadyExternals = function (limit) {
        if (limit === void 0) { limit = Infinity; }
        var indexToSkip = {};
        var externalsByIndex = {};
        this.every(function (ex, index) {
            if (limit <= 0)
                return null;
            if (ex instanceof ExternalExpression) {
                if (indexToSkip[index])
                    return null;
                if (!ex.external.suppress) {
                    limit--;
                    externalsByIndex[index] = {
                        external: ex.external,
                        terminal: true
                    };
                }
            }
            else if (ex instanceof ChainableExpression) {
                var h = ex._headExternal();
                if (h) {
                    if (h.allGood) {
                        limit--;
                        externalsByIndex[index + h.offset] = { external: h.external };
                        return true;
                    }
                    else {
                        indexToSkip[index + h.offset] = true;
                        return null;
                    }
                }
            }
            else if (ex instanceof LiteralExpression && ex.type === 'DATASET') {
                var datasetExternals = ex.value.getReadyExternals(limit);
                var size = sizeOfDatasetExternalAlterations(datasetExternals);
                if (size) {
                    limit -= size;
                    externalsByIndex[index] = datasetExternals;
                }
                return null;
            }
            return null;
        });
        return externalsByIndex;
    };
    Expression.prototype.applyReadyExternals = function (alterations) {
        return this.substitute(function (ex, index) {
            var alteration = alterations[index];
            if (!alteration)
                return null;
            if (Array.isArray(alteration)) {
                return r(ex.getLiteralValue().applyReadyExternals(alteration));
            }
            else {
                return r(alteration.result);
            }
        }).simplify();
    };
    Expression.prototype._headExternal = function () {
        var ex = this;
        var allGood = true;
        var offset = 0;
        while (ex instanceof ChainableExpression) {
            allGood = allGood && (ex.op === 'filter' ? ex.argumentsResolvedWithoutExternals() : ex.argumentsResolved());
            ex = ex.operand;
            offset++;
        }
        if (ex instanceof ExternalExpression) {
            return {
                allGood: allGood,
                external: ex.external,
                offset: offset
            };
        }
        else {
            return null;
        }
    };
    Expression.prototype.getHeadOperand = function () {
        return this;
    };
    Expression.prototype.getFreeReferences = function () {
        var freeReferences = [];
        this.forEach(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression && nestDiff <= ex.nest) {
                freeReferences.push(repeat('^', ex.nest - nestDiff) + ex.name);
            }
        });
        return deduplicateSort(freeReferences);
    };
    Expression.prototype.getFreeReferenceIndexes = function () {
        var freeReferenceIndexes = [];
        this.forEach(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression && nestDiff <= ex.nest) {
                freeReferenceIndexes.push(index);
            }
        });
        return freeReferenceIndexes;
    };
    Expression.prototype.incrementNesting = function (by) {
        if (by === void 0) { by = 1; }
        var freeReferenceIndexes = this.getFreeReferenceIndexes();
        if (freeReferenceIndexes.length === 0)
            return this;
        return this.substitute(function (ex, index) {
            if (ex instanceof RefExpression && freeReferenceIndexes.indexOf(index) !== -1) {
                return ex.incrementNesting(by);
            }
            return null;
        });
    };
    Expression.prototype.simplify = function () {
        return this;
    };
    Expression.prototype.every = function (iter, thisArg) {
        return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
    };
    Expression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
        var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
        if (pass != null) {
            return pass;
        }
        else {
            indexer.index++;
        }
        return true;
    };
    Expression.prototype.some = function (iter, thisArg) {
        var _this = this;
        return !this.every(function (ex, index, depth, nestDiff) {
            var v = iter.call(_this, ex, index, depth, nestDiff);
            return (v == null) ? null : !v;
        }, thisArg);
    };
    Expression.prototype.forEach = function (iter, thisArg) {
        var _this = this;
        this.every(function (ex, index, depth, nestDiff) {
            iter.call(_this, ex, index, depth, nestDiff);
            return null;
        }, thisArg);
    };
    Expression.prototype.substitute = function (substitutionFn, typeContext) {
        if (typeContext === void 0) { typeContext = null; }
        return this._substituteHelper(substitutionFn, { index: 0 }, 0, 0, typeContext).expression;
    };
    Expression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        return {
            expression: this,
            typeContext: this.updateTypeContextIfNeeded(typeContext)
        };
    };
    Expression.prototype.fullyDefined = function () {
        return true;
    };
    Expression.prototype.getJSFn = function (datumVar) {
        if (datumVar === void 0) { datumVar = 'd[]'; }
        var type = this.type;
        var jsEx = this.getJS(datumVar);
        var body;
        if (type === 'NUMBER' || type === 'NUMBER_RANGE' || type === 'TIME') {
            body = "_=" + jsEx + ";return isNaN(_)?null:_";
        }
        else {
            body = "return " + jsEx + ";";
        }
        return "function(" + datumVar.replace('[]', '') + "){var _,_2;" + body + "}";
    };
    Expression.prototype.extractFromAnd = function (matchFn) {
        if (this.type !== 'BOOLEAN')
            return null;
        if (matchFn(this)) {
            return {
                extract: this,
                rest: Expression.TRUE
            };
        }
        else {
            return {
                extract: Expression.TRUE,
                rest: this
            };
        }
    };
    Expression.prototype.breakdownByDataset = function (tempNamePrefix) {
        if (tempNamePrefix === void 0) { tempNamePrefix = 'b'; }
        throw new Error('ToDo');
    };
    Expression.prototype.getLiteralValue = function () {
        return null;
    };
    Expression.prototype.upgradeToType = function (targetType) {
        return this;
    };
    Expression.prototype.performAction = function (action) {
        var _this = this;
        return action.substitute(function (ex) { return ex.equals(Expression._) ? _this : null; });
    };
    Expression.prototype.performActions = function (actions) {
        var ex = this;
        for (var _i = 0, actions_1 = actions; _i < actions_1.length; _i++) {
            var action = actions_1[_i];
            ex = ex.performAction(action);
        }
        return ex;
    };
    Expression.prototype.getOptions = function () {
        return this.options || {};
    };
    Expression.prototype.setOptions = function (options) {
        var value = this.valueOf();
        value.options = options;
        return Expression.fromValue(value);
    };
    Expression.prototype.setOption = function (optionKey, optionValue) {
        var newOptions = Object.assign({}, this.getOptions());
        newOptions[optionKey] = optionValue;
        return this.setOptions(newOptions);
    };
    Expression.prototype._mkChain = function (ExpressionClass, exs) {
        var cur = this;
        for (var _i = 0, exs_1 = exs; _i < exs_1.length; _i++) {
            var ex = exs_1[_i];
            cur = new ExpressionClass({
                operand: cur,
                expression: ex instanceof Expression ? ex : Expression.fromJSLoose(ex)
            });
        }
        return cur;
    };
    Expression.prototype.add = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(AddExpression, exs);
    };
    Expression.prototype.subtract = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(SubtractExpression, exs);
    };
    Expression.prototype.negate = function () {
        return Expression.ZERO.subtract(this);
    };
    Expression.prototype.multiply = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(MultiplyExpression, exs);
    };
    Expression.prototype.divide = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(DivideExpression, exs);
    };
    Expression.prototype.reciprocate = function () {
        return Expression.ONE.divide(this);
    };
    Expression.prototype.sqrt = function () {
        return this.power(0.5);
    };
    Expression.prototype.power = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(PowerExpression, exs);
    };
    Expression.prototype.log = function (ex) {
        if (ex === void 0) { ex = Math.E; }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LogExpression({ operand: this, expression: ex });
    };
    Expression.prototype.ln = function () {
        return new LogExpression({ operand: this, expression: r(Math.E) });
    };
    Expression.prototype.then = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new ThenExpression({ operand: this, expression: ex });
    };
    Expression.prototype.fallback = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new FallbackExpression({ operand: this, expression: ex });
    };
    Expression.prototype.is = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new IsExpression({ operand: this, expression: ex });
    };
    Expression.prototype.isnt = function (ex) {
        return this.is(ex).not();
    };
    Expression.prototype.lessThan = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LessThanExpression({ operand: this, expression: ex });
    };
    Expression.prototype.lessThanOrEqual = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LessThanOrEqualExpression({ operand: this, expression: ex });
    };
    Expression.prototype.greaterThan = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new GreaterThanExpression({ operand: this, expression: ex });
    };
    Expression.prototype.greaterThanOrEqual = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new GreaterThanOrEqualExpression({ operand: this, expression: ex });
    };
    Expression.prototype.contains = function (ex, compare) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        if (compare)
            compare = getString(compare);
        return new ContainsExpression({ operand: this, expression: ex, compare: compare });
    };
    Expression.prototype.match = function (re) {
        return new MatchExpression({ operand: this, regexp: getString(re) });
    };
    Expression.prototype.in = function (ex) {
        if (arguments.length === 2) {
            return this.overlap(ex, arguments[1]);
        }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        if (Range.isRangeType(ex.type)) {
            return new OverlapExpression({ operand: this, expression: ex });
        }
        return new InExpression({ operand: this, expression: ex });
    };
    Expression.prototype.overlap = function (ex, snd) {
        if (arguments.length === 2) {
            ex = getValue(ex);
            snd = getValue(snd);
            if (typeof ex === 'string') {
                var parse = parseISODate(ex, Expression.defaultParserTimezone);
                if (parse)
                    ex = parse;
            }
            if (typeof snd === 'string') {
                var parse = parseISODate(snd, Expression.defaultParserTimezone);
                if (parse)
                    snd = parse;
            }
            if (typeof ex === 'number' && typeof snd === 'number') {
                ex = new NumberRange({ start: ex, end: snd });
            }
            else if (ex.toISOString && snd.toISOString) {
                ex = new TimeRange({ start: ex, end: snd });
            }
            else if (typeof ex === 'string' && typeof snd === 'string') {
                ex = new StringRange({ start: ex, end: snd });
            }
            else {
                throw new Error('uninterpretable IN parameters');
            }
        }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new OverlapExpression({ operand: this, expression: ex });
    };
    Expression.prototype.not = function () {
        return new NotExpression({ operand: this });
    };
    Expression.prototype.and = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(AndExpression, exs);
    };
    Expression.prototype.or = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(OrExpression, exs);
    };
    Expression.prototype.substr = function (position, len) {
        return new SubstrExpression({ operand: this, position: getNumber(position), len: getNumber(len) });
    };
    Expression.prototype.extract = function (re) {
        return new ExtractExpression({ operand: this, regexp: getString(re) });
    };
    Expression.prototype.concat = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(ConcatExpression, exs);
    };
    Expression.prototype.lookup = function (lookupFn) {
        return new LookupExpression({ operand: this, lookupFn: getString(lookupFn) });
    };
    Expression.prototype.indexOf = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new IndexOfExpression({ operand: this, expression: ex });
    };
    Expression.prototype.transformCase = function (transformType) {
        return new TransformCaseExpression({ operand: this, transformType: getString(transformType) });
    };
    Expression.prototype.customTransform = function (custom, outputType) {
        if (!custom)
            throw new Error("Must provide an extraction function name for custom transform");
        outputType = outputType !== undefined ? getString(outputType) : null;
        return new CustomTransformExpression({ operand: this, custom: getString(custom), outputType: outputType });
    };
    Expression.prototype.numberBucket = function (size, offset) {
        if (offset === void 0) { offset = 0; }
        return new NumberBucketExpression({ operand: this, size: getNumber(size), offset: getNumber(offset) });
    };
    Expression.prototype.absolute = function () {
        return new AbsoluteExpression({ operand: this });
    };
    Expression.prototype.length = function () {
        return new LengthExpression({ operand: this });
    };
    Expression.prototype.timeBucket = function (duration, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeBucketExpression({ operand: this, duration: duration, timezone: timezone });
    };
    Expression.prototype.timeFloor = function (duration, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeFloorExpression({ operand: this, duration: duration, timezone: timezone });
    };
    Expression.prototype.timeShift = function (duration, step, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        step = typeof step !== 'undefined' ? getNumber(step) : null;
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeShiftExpression({ operand: this, duration: duration, step: step, timezone: timezone });
    };
    Expression.prototype.timeRange = function (duration, step, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        step = typeof step !== 'undefined' ? getNumber(step) : null;
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeRangeExpression({ operand: this, duration: duration, step: step, timezone: timezone });
    };
    Expression.prototype.timePart = function (part, timezone) {
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimePartExpression({ operand: this, part: getString(part), timezone: timezone });
    };
    Expression.prototype.cast = function (outputType) {
        return new CastExpression({ operand: this, outputType: getString(outputType) });
    };
    Expression.prototype.cardinality = function () {
        return new CardinalityExpression({ operand: this });
    };
    Expression.prototype.filter = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new FilterExpression({ operand: this, expression: ex });
    };
    Expression.prototype.split = function (splits, name, dataName) {
        if (arguments.length === 3 ||
            ((arguments.length === 2 || arguments.length === 1) && (typeof splits === 'string' || typeof splits.op === 'string'))) {
            name = arguments.length === 1 ? 'split' : getString(name);
            var realSplits = Object.create(null);
            realSplits[name] = splits;
            splits = realSplits;
        }
        else {
            dataName = name;
        }
        var parsedSplits = Object.create(null);
        for (var k in splits) {
            if (!hasOwnProp(splits, k))
                continue;
            var ex = splits[k];
            parsedSplits[k] = ex instanceof Expression ? ex : Expression.fromJSLoose(ex);
        }
        dataName = dataName ? getString(dataName) : getDataName(this);
        if (!dataName)
            throw new Error("could not guess data name in `split`, please provide one explicitly");
        return new SplitExpression({ operand: this, splits: parsedSplits, dataName: dataName });
    };
    Expression.prototype.apply = function (name, ex) {
        if (arguments.length < 2)
            throw new Error('invalid arguments to .apply, did you forget to specify a name?');
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new ApplyExpression({ operand: this, name: getString(name), expression: ex });
    };
    Expression.prototype.sort = function (ex, direction) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new SortExpression({ operand: this, expression: ex, direction: direction ? getString(direction) : null });
    };
    Expression.prototype.limit = function (value) {
        return new LimitExpression({ operand: this, value: getNumber(value) });
    };
    Expression.prototype.select = function () {
        var attributes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            attributes[_i] = arguments[_i];
        }
        attributes = (attributes.length === 1 && Array.isArray(attributes[0])) ? attributes[0] : attributes.map(getString);
        return new SelectExpression({ operand: this, attributes: attributes });
    };
    Expression.prototype.count = function () {
        if (arguments.length)
            throw new Error('.count() should not have arguments, did you want to .filter().count() ?');
        return new CountExpression({ operand: this });
    };
    Expression.prototype.sum = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new SumExpression({ operand: this, expression: ex });
    };
    Expression.prototype.min = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new MinExpression({ operand: this, expression: ex });
    };
    Expression.prototype.max = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new MaxExpression({ operand: this, expression: ex });
    };
    Expression.prototype.average = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new AverageExpression({ operand: this, expression: ex });
    };
    Expression.prototype.countDistinct = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new CountDistinctExpression({ operand: this, expression: ex });
    };
    Expression.prototype.quantile = function (ex, value, tuning) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new QuantileExpression({ operand: this, expression: ex, value: getNumber(value), tuning: tuning ? getString(tuning) : null });
    };
    Expression.prototype.collect = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new CollectExpression({ operand: this, expression: ex });
    };
    Expression.prototype.customAggregate = function (custom) {
        return new CustomAggregateExpression({ operand: this, custom: getString(custom) });
    };
    Expression.prototype.join = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new JoinExpression({ operand: this, expression: ex });
    };
    Expression.prototype.needsEnvironment = function () {
        return false;
    };
    Expression.prototype.defineEnvironment = function (environment) {
        if (!environment.timezone)
            environment = { timezone: Timezone.UTC };
        if (typeof environment.timezone === 'string')
            environment = { timezone: Timezone.fromJS(environment.timezone) };
        return this.substitute(function (ex) {
            if (ex.needsEnvironment()) {
                return ex.defineEnvironment(environment);
            }
            return null;
        });
    };
    Expression.prototype.referenceCheck = function (context) {
        return this.changeInTypeContext(getFullTypeFromDatum(context));
    };
    Expression.prototype.definedInTypeContext = function (typeContext) {
        try {
            this.changeInTypeContext(typeContext);
        }
        catch (e) {
            return false;
        }
        return true;
    };
    Expression.prototype.referenceCheckInTypeContext = function (typeContext) {
        console.warn("referenceCheckInTypeContext is deprecated, use changeInTypeContext instead");
        return this.changeInTypeContext(typeContext);
    };
    Expression.prototype.changeInTypeContext = function (typeContext) {
        return this.substitute(function (ex, index, depth, nestDiff, typeContext) {
            if (ex instanceof RefExpression) {
                return ex.changeInTypeContext(typeContext);
            }
            return null;
        }, typeContext);
    };
    Expression.prototype.updateTypeContext = function (typeContext, extra) {
        return typeContext;
    };
    Expression.prototype.updateTypeContextIfNeeded = function (typeContext, extra) {
        return typeContext ? this.updateTypeContext(typeContext, extra) : null;
    };
    Expression.prototype.resolve = function (context, ifNotFound) {
        if (ifNotFound === void 0) { ifNotFound = 'throw'; }
        var expressions = Object.create(null);
        for (var k in context) {
            if (!hasOwnProp(context, k))
                continue;
            var value = context[k];
            if (value instanceof External) {
                expressions[k] = new ExternalExpression({ external: value });
            }
            else if (value instanceof Expression) {
                expressions[k] = value;
            }
            else {
                expressions[k] = new LiteralExpression({ value: value });
            }
        }
        return this.resolveWithExpressions(expressions, ifNotFound);
    };
    Expression.prototype.resolveWithExpressions = function (expressions, ifNotFound) {
        if (ifNotFound === void 0) { ifNotFound = 'throw'; }
        return this.substitute(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression) {
                var nest = ex.nest, ignoreCase = ex.ignoreCase, name_3 = ex.name;
                if (nestDiff === nest) {
                    var foundExpression = null;
                    var valueFound = false;
                    var property = ignoreCase ? RefExpression.findPropertyCI(expressions, name_3) : RefExpression.findProperty(expressions, name_3);
                    if (property != null) {
                        foundExpression = expressions[property];
                        valueFound = true;
                    }
                    if (foundExpression instanceof ExternalExpression) {
                        var mode = foundExpression.external.mode;
                        if (mode === 'split') {
                            return ex;
                        }
                        if (nest > 0 && mode !== 'raw') {
                            return ex;
                        }
                    }
                    if (valueFound) {
                        return foundExpression;
                    }
                    else if (ifNotFound === 'throw') {
                        throw new Error("could not resolve " + ex + " because is was not in the context");
                    }
                    else if (ifNotFound === 'null') {
                        return Expression.NULL;
                    }
                    else if (ifNotFound === 'leave') {
                        return ex;
                    }
                }
                else if (nestDiff < nest) {
                    throw new Error("went too deep during resolve on: " + ex);
                }
            }
            return null;
        });
    };
    Expression.prototype.resolved = function () {
        return this.every(function (ex, index, depth, nestDiff) {
            return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null;
        });
    };
    Expression.prototype.resolvedWithoutExternals = function () {
        return this.every(function (ex, index, depth, nestDiff) {
            if (ex instanceof ExternalExpression)
                return false;
            return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null;
        });
    };
    Expression.prototype.noRefs = function () {
        return this.every(function (ex) {
            if (ex instanceof RefExpression)
                return false;
            return null;
        });
    };
    Expression.prototype.isAggregate = function () {
        return false;
    };
    Expression.prototype.decomposeAverage = function (countEx) {
        return this.substitute(function (ex) {
            if (ex instanceof AverageExpression) {
                return ex.decomposeAverage(countEx);
            }
            return null;
        });
    };
    Expression.prototype.distribute = function () {
        return this.substitute(function (ex, index) {
            if (index === 0)
                return null;
            var distributedEx = ex.distribute();
            if (distributedEx === ex)
                return null;
            return distributedEx;
        }).simplify();
    };
    Expression.prototype.maxPossibleSplitValues = function () {
        return this.type === 'BOOLEAN' ? 3 : Infinity;
    };
    Expression.prototype._initialPrepare = function (context, environment) {
        return this.defineEnvironment(environment)
            .referenceCheck(context)
            .resolve(context)
            .simplify();
    };
    Expression.prototype.simulate = function (context, options) {
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        failIfIntrospectNeededInDatum(context);
        var readyExpression = this._initialPrepare(context, options);
        if (readyExpression instanceof ExternalExpression) {
            readyExpression = readyExpression.unsuppress();
        }
        return readyExpression._computeResolvedSimulate(options, []);
    };
    Expression.prototype.simulateQueryPlan = function (context, options) {
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        failIfIntrospectNeededInDatum(context);
        var readyExpression = this._initialPrepare(context, options);
        if (readyExpression instanceof ExternalExpression) {
            readyExpression = readyExpression.unsuppress();
        }
        var simulatedQueryGroups = [];
        readyExpression._computeResolvedSimulate(options, simulatedQueryGroups);
        return simulatedQueryGroups;
    };
    Expression.prototype._computeResolvedSimulate = function (options, simulatedQueryGroups) {
        var _a = options.maxComputeCycles, maxComputeCycles = _a === void 0 ? 5 : _a, _b = options.maxQueries, maxQueries = _b === void 0 ? 500 : _b, maxRows = options.maxRows, _c = options.concurrentQueryLimit, concurrentQueryLimit = _c === void 0 ? Infinity : _c;
        var ex = this;
        var readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        var computeCycles = 0;
        var queries = 0;
        var _loop_1 = function () {
            var simulatedQueryGroup = [];
            fillExpressionExternalAlteration(readyExternals, function (external, terminal) {
                if (queries < maxQueries) {
                    queries++;
                    return external.simulateValue(terminal, simulatedQueryGroup);
                }
                else {
                    queries++;
                    return null;
                }
            });
            simulatedQueryGroups.push(simulatedQueryGroup);
            ex = ex.applyReadyExternals(readyExternals);
            var literalValue = ex.getLiteralValue();
            if (maxRows && literalValue instanceof Dataset) {
                ex = r(literalValue.depthFirstTrimTo(maxRows));
            }
            readyExternals = ex.getReadyExternals(concurrentQueryLimit);
            computeCycles++;
        };
        while (Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queries < maxQueries) {
            _loop_1();
        }
        return ex.getLiteralValue();
    };
    Expression.prototype.compute = function (context, options, computeContext) {
        var _this = this;
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        if (computeContext === void 0) { computeContext = {}; }
        return Promise.resolve(null)
            .then(function () {
            return introspectDatum(context);
        })
            .then(function (introspectedContext) {
            var readyExpression = _this._initialPrepare(introspectedContext, options);
            if (readyExpression instanceof ExternalExpression) {
                readyExpression = readyExpression.unsuppress();
            }
            return readyExpression._computeResolved(options, computeContext);
        });
    };
    Expression.prototype.computeStream = function (context, options, computeContext) {
        var _this = this;
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        var pt = new PassThrough({ objectMode: true });
        var rawQueries = options.rawQueries;
        introspectDatum(context)
            .then(function (introspectedContext) {
            var readyExpression = _this._initialPrepare(introspectedContext, options);
            if (readyExpression instanceof ExternalExpression) {
                pipeWithError(readyExpression.external.queryValueStream(true, rawQueries, computeContext), pt);
                return;
            }
            readyExpression._computeResolved(options, computeContext)
                .then(function (v) {
                var i = iteratorFactory(v);
                var bit;
                while (bit = i()) {
                    pt.write(bit);
                }
                pt.end();
            });
        })
            .catch(function (e) {
            pt.emit('error', e);
        });
        return pt;
    };
    Expression.prototype._computeResolved = function (options, computeContext) {
        var _this = this;
        var rawQueries = options.rawQueries, _a = options.maxComputeCycles, maxComputeCycles = _a === void 0 ? 5 : _a, _b = options.maxQueries, maxQueries = _b === void 0 ? 500 : _b, maxRows = options.maxRows, _c = options.concurrentQueryLimit, concurrentQueryLimit = _c === void 0 ? Infinity : _c;
        var ex = this;
        var readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        var computeCycles = 0;
        var queriesMade = 0;
        return promiseWhile(function () { return Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queriesMade < maxQueries; }, function () { return tslib_1.__awaiter(_this, void 0, void 0, function () {
            var readyExternalsFilled, literalValue;
            return tslib_1.__generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, fillExpressionExternalAlterationAsync(readyExternals, function (external, terminal) {
                            if (queriesMade < maxQueries) {
                                queriesMade++;
                                return external.queryValue(terminal, rawQueries, computeContext);
                            }
                            else {
                                queriesMade++;
                                return Promise.resolve(null);
                            }
                        })];
                    case 1:
                        readyExternalsFilled = _a.sent();
                        ex = ex.applyReadyExternals(readyExternalsFilled);
                        literalValue = ex.getLiteralValue();
                        if (maxRows && literalValue instanceof Dataset) {
                            ex = r(literalValue.depthFirstTrimTo(maxRows));
                        }
                        readyExternals = ex.getReadyExternals(concurrentQueryLimit);
                        computeCycles++;
                        return [2];
                }
            });
        }); })
            .then(function () {
            if (!ex.isOp('literal'))
                throw new Error("something went wrong, did not get literal: " + ex);
            return ex.getLiteralValue();
        });
    };
    Expression.defaultParserTimezone = Timezone.UTC;
    Expression.classMap = {};
    return Expression;
}());
export { Expression };
var ChainableExpression = (function (_super) {
    tslib_1.__extends(ChainableExpression, _super);
    function ChainableExpression(value, dummy) {
        if (dummy === void 0) { dummy = null; }
        var _this = _super.call(this, value, dummy) || this;
        _this.operand = value.operand || Expression._;
        return _this;
    }
    ChainableExpression.jsToValue = function (js) {
        var value = Expression.jsToValue(js);
        value.operand = js.operand ? Expression.fromJS(js.operand) : Expression._;
        return value;
    };
    ChainableExpression.prototype._checkTypeAgainstTypes = function (name, type, neededTypes) {
        if (type && type !== 'NULL' && neededTypes.indexOf(type) === -1) {
            if (neededTypes.length === 1) {
                throw new Error(this.op + " must have " + name + " of type " + neededTypes[0] + " (is " + type + ")");
            }
            else {
                throw new Error(this.op + " must have " + name + " of type " + neededTypes.join(' or ') + " (is " + type + ")");
            }
        }
    };
    ChainableExpression.prototype._checkOperandTypes = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('operand', Set.unwrapSetType(this.operand.type), neededTypes);
    };
    ChainableExpression.prototype._checkOperandTypesStrict = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('operand', this.operand.type, neededTypes);
    };
    ChainableExpression.prototype._bumpOperandToTime = function () {
        if (this.operand.type === 'STRING') {
            this.operand = this.operand.upgradeToType('TIME');
        }
    };
    ChainableExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.operand = this.operand;
        return value;
    };
    ChainableExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        if (!this.operand.equals(Expression._)) {
            js.operand = this.operand.toJS();
        }
        return js;
    };
    ChainableExpression.prototype._toStringParameters = function (indent) {
        return [];
    };
    ChainableExpression.prototype.toString = function (indent) {
        return this.operand.toString(indent) + "." + this.op + "(" + this._toStringParameters(indent).join(',') + ")";
    };
    ChainableExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.operand.equals(other.operand);
    };
    ChainableExpression.prototype.changeOperand = function (operand) {
        if (this.operand === operand || this.operand.equals(operand))
            return this;
        var value = this.valueOf();
        value.operand = operand;
        delete value.simple;
        return Expression.fromValue(value);
    };
    ChainableExpression.prototype.swapWithOperand = function () {
        var operand = this.operand;
        if (operand instanceof ChainableExpression) {
            return operand.changeOperand(this.changeOperand(operand.operand));
        }
        else {
            throw new Error('operand must be chainable');
        }
    };
    ChainableExpression.prototype.getAction = function () {
        return this.changeOperand(Expression._);
    };
    ChainableExpression.prototype.getHeadOperand = function () {
        var iter = this.operand;
        while (iter instanceof ChainableExpression)
            iter = iter.operand;
        return iter;
    };
    ChainableExpression.prototype.getArgumentExpressions = function () {
        return [];
    };
    ChainableExpression.prototype.expressionCount = function () {
        var sum = _super.prototype.expressionCount.call(this) + this.operand.expressionCount();
        this.getArgumentExpressions().forEach(function (ex) { return sum += ex.expressionCount(); });
        return sum;
    };
    ChainableExpression.prototype.argumentsResolved = function () {
        return this.getArgumentExpressions().every(function (ex) { return ex.resolved(); });
    };
    ChainableExpression.prototype.argumentsResolvedWithoutExternals = function () {
        return this.getArgumentExpressions().every(function (ex) { return ex.resolvedWithoutExternals(); });
    };
    ChainableExpression.prototype.getFn = function () {
        var _this = this;
        return function (d) { return _this.calc(d); };
    };
    ChainableExpression.prototype._calcChainableHelper = function (operandValue) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal');
    };
    ChainableExpression.prototype.calc = function (datum) {
        return this._calcChainableHelper(this.operand.calc(datum));
    };
    ChainableExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.getJS = function (datumVar) {
        return this._getJSChainableHelper(this.operand.getJS(datumVar));
    };
    ChainableExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.getSQL = function (dialect) {
        return this._getSQLChainableHelper(dialect, this.operand.getSQL(dialect));
    };
    ChainableExpression.prototype.pushIntoExternal = function () {
        var operand = this.operand;
        if (operand instanceof ExternalExpression) {
            return operand.addExpression(this.getAction());
        }
        return null;
    };
    ChainableExpression.prototype.specialSimplify = function () {
        return this;
    };
    ChainableExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpler = this.changeOperand(this.operand.simplify());
        if (simpler.fullyDefined()) {
            return r(simpler.calc({}));
        }
        var specialSimpler = simpler.specialSimplify();
        if (specialSimpler === simpler) {
            simpler = specialSimpler.markSimple();
        }
        else {
            simpler = specialSimpler.simplify();
        }
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler;
    };
    ChainableExpression.prototype.isNester = function () {
        return false;
    };
    ChainableExpression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
        var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
        if (pass != null) {
            return pass;
        }
        else {
            indexer.index++;
        }
        depth++;
        var operand = this.operand;
        if (!operand._everyHelper(iter, thisArg, indexer, depth, nestDiff))
            return false;
        var nestDiffNext = nestDiff + Number(this.isNester());
        return this.getArgumentExpressions().every(function (ex) { return ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext); });
    };
    ChainableExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        depth++;
        var operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
        var updatedThis = this.changeOperand(operandSubs.expression);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
        };
    };
    return ChainableExpression;
}(Expression));
export { ChainableExpression };
var ChainableUnaryExpression = (function (_super) {
    tslib_1.__extends(ChainableUnaryExpression, _super);
    function ChainableUnaryExpression(value, dummy) {
        if (dummy === void 0) { dummy = null; }
        var _this = _super.call(this, value, dummy) || this;
        if (!value.expression)
            throw new Error("must have an expression");
        _this.expression = value.expression;
        return _this;
    }
    ChainableUnaryExpression.jsToValue = function (js) {
        var value = ChainableExpression.jsToValue(js);
        value.expression = Expression.fromJS(js.expression);
        return value;
    };
    ChainableUnaryExpression.prototype._checkExpressionTypes = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('expression', Set.unwrapSetType(this.expression.type), neededTypes);
    };
    ChainableUnaryExpression.prototype._checkExpressionTypesStrict = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('expression', this.expression.type, neededTypes);
    };
    ChainableUnaryExpression.prototype._checkOperandExpressionTypesAlign = function () {
        var operandType = Set.unwrapSetType(this.operand.type);
        var expressionType = Set.unwrapSetType(this.expression.type);
        if (!operandType || operandType === 'NULL' || !expressionType || expressionType === 'NULL' || operandType === expressionType)
            return;
        throw new Error(this.op + " must have matching types (are " + this.operand.type + ", " + this.expression.type + ")");
    };
    ChainableUnaryExpression.prototype._bumpOperandExpressionToTime = function () {
        if (this.expression.type === 'TIME' && this.operand.type === 'STRING') {
            this.operand = this.operand.upgradeToType('TIME');
        }
        if (this.operand.type === 'TIME' && this.expression.type === 'STRING') {
            this.expression = this.expression.upgradeToType('TIME');
        }
    };
    ChainableUnaryExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.expression = this.expression;
        return value;
    };
    ChainableUnaryExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.expression = this.expression.toJS();
        return js;
    };
    ChainableUnaryExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent)];
    };
    ChainableUnaryExpression.prototype.toString = function (indent) {
        return this.operand.toString(indent) + "." + this.op + "(" + this._toStringParameters(indent).join(',') + ")";
    };
    ChainableUnaryExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.expression.equals(other.expression);
    };
    ChainableUnaryExpression.prototype.changeExpression = function (expression) {
        if (this.expression === expression || this.expression.equals(expression))
            return this;
        var value = this.valueOf();
        value.expression = expression;
        delete value.simple;
        return Expression.fromValue(value);
    };
    ChainableUnaryExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.isOp('literal');
    };
    ChainableUnaryExpression.prototype.calc = function (datum) {
        return this._calcChainableUnaryHelper(this.operand.calc(datum), this.isNester() ? null : this.expression.calc(datum));
    };
    ChainableUnaryExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.getJS = function (datumVar) {
        return this._getJSChainableUnaryHelper(this.operand.getJS(datumVar), this.expression.getJS(datumVar));
    };
    ChainableUnaryExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.getSQL = function (dialect) {
        return this._getSQLChainableUnaryHelper(dialect, this.operand.getSQL(dialect), this.expression.getSQL(dialect));
    };
    ChainableUnaryExpression.prototype.getExpressionList = function () {
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        var expressionList = [expression];
        var iter = operand;
        while (iter.op === op) {
            expressionList.unshift(iter.expression);
            iter = iter.operand;
        }
        expressionList.unshift(iter);
        return expressionList;
    };
    ChainableUnaryExpression.prototype.isCommutative = function () {
        return false;
    };
    ChainableUnaryExpression.prototype.isAssociative = function () {
        return false;
    };
    ChainableUnaryExpression.prototype.associateLeft = function () {
        if (!this.isAssociative())
            return null;
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        if (op !== expression.op)
            return null;
        var MyClass = this.constructor;
        return new MyClass({
            operand: new MyClass({
                operand: operand,
                expression: expression.operand
            }),
            expression: expression.expression
        });
    };
    ChainableUnaryExpression.prototype.associateRightIfSimpler = function () {
        if (!this.isAssociative())
            return null;
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        if (op !== operand.op)
            return null;
        var MyClass = this.constructor;
        var simpleExpression = new MyClass({
            operand: operand.expression,
            expression: expression
        }).simplify();
        if (simpleExpression instanceof LiteralExpression) {
            return new MyClass({
                operand: operand.operand,
                expression: simpleExpression
            }).simplify();
        }
        else {
            return null;
        }
    };
    ChainableUnaryExpression.prototype.pushIntoExternal = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand instanceof ExternalExpression) {
            return operand.addExpression(this.getAction());
        }
        if (expression instanceof ExternalExpression) {
            return expression.prePush(this.changeExpression(Expression._));
        }
        return null;
    };
    ChainableUnaryExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpleOperand = this.operand.simplify();
        var simpleExpression = this.expression.simplify();
        var simpler = this.changeOperand(simpleOperand).changeExpression(simpleExpression);
        if (simpler.fullyDefined())
            return r(simpler.calc({}));
        if (this.isCommutative() && simpleOperand instanceof LiteralExpression) {
            var MyClass = this.constructor;
            var myValue = this.valueOf();
            myValue.operand = simpleExpression;
            myValue.expression = simpleOperand;
            return new MyClass(myValue).simplify();
        }
        var assLeft = simpler.associateLeft();
        if (assLeft)
            return assLeft.simplify();
        if (simpler instanceof ChainableUnaryExpression) {
            var specialSimpler = simpler.specialSimplify();
            if (specialSimpler !== simpler) {
                return specialSimpler.simplify();
            }
            else {
                simpler = specialSimpler;
            }
            if (simpler instanceof ChainableUnaryExpression) {
                var assRight = simpler.associateRightIfSimpler();
                if (assRight)
                    return assRight;
            }
        }
        simpler = simpler.markSimple();
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler;
    };
    ChainableUnaryExpression.prototype.getArgumentExpressions = function () {
        return [this.expression];
    };
    ChainableUnaryExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        depth++;
        var operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
        var nestDiffNext = nestDiff + Number(this.isNester());
        var expressionSubs = this.expression._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, this.isNester() ? operandSubs.typeContext : typeContext);
        var updatedThis = this.changeOperand(operandSubs.expression).changeExpression(expressionSubs.expression);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext, expressionSubs.typeContext)
        };
    };
    return ChainableUnaryExpression;
}(ChainableExpression));
export { ChainableUnaryExpression };
