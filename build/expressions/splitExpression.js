import * as tslib_1 from "tslib";
import * as hasOwnProp from 'has-own-prop';
import { immutableLookupsEqual } from 'immutable-class';
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression, r } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var SplitExpression = (function (_super) {
    tslib_1.__extends(SplitExpression, _super);
    function SplitExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("split");
        _this._checkOperandTypes('DATASET');
        var splits = parameters.splits;
        if (!splits)
            throw new Error('must have splits');
        _this.splits = splits;
        _this.keys = Object.keys(splits).sort();
        if (!_this.keys.length)
            throw new Error('must have at least one split');
        _this.dataName = parameters.dataName;
        _this.type = 'DATASET';
        return _this;
    }
    SplitExpression.fromJS = function (parameters) {
        var _a;
        var value = ChainableExpression.jsToValue(parameters);
        var splits;
        if (parameters.expression && parameters.name) {
            splits = (_a = {}, _a[parameters.name] = parameters.expression, _a);
        }
        else {
            splits = parameters.splits;
        }
        value.splits = Expression.expressionLookupFromJS(splits);
        value.dataName = parameters.dataName;
        return new SplitExpression(value);
    };
    SplitExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.splits = this.splits;
        value.dataName = this.dataName;
        return value;
    };
    SplitExpression.prototype.toJS = function () {
        var splits = this.splits;
        var js = _super.prototype.toJS.call(this);
        if (this.isMultiSplit()) {
            js.splits = Expression.expressionLookupToJS(splits);
        }
        else {
            for (var name_1 in splits) {
                js.name = name_1;
                js.expression = splits[name_1].toJS();
            }
        }
        js.dataName = this.dataName;
        return js;
    };
    SplitExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            immutableLookupsEqual(this.splits, other.splits) &&
            this.dataName === other.dataName;
    };
    SplitExpression.prototype.changeSplits = function (splits) {
        if (immutableLookupsEqual(this.splits, splits))
            return this;
        var value = this.valueOf();
        value.splits = splits;
        return new SplitExpression(value);
    };
    SplitExpression.prototype.numSplits = function () {
        return this.keys.length;
    };
    SplitExpression.prototype.isMultiSplit = function () {
        return this.numSplits() > 1;
    };
    SplitExpression.prototype._toStringParameters = function (indent) {
        if (this.isMultiSplit()) {
            var splits = this.splits;
            var splitStrings = [];
            for (var name_2 in splits) {
                splitStrings.push(name_2 + ": " + splits[name_2]);
            }
            return [splitStrings.join(', '), this.dataName];
        }
        else {
            return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
        }
    };
    SplitExpression.prototype.updateTypeContext = function (typeContext) {
        var newDatasetType = {};
        this.mapSplits(function (name, expression) {
            newDatasetType[name] = {
                type: Set.unwrapSetType(expression.type)
            };
        });
        newDatasetType[this.dataName] = typeContext;
        return {
            parent: typeContext.parent,
            type: 'DATASET',
            datasetType: newDatasetType
        };
    };
    SplitExpression.prototype.firstSplitName = function () {
        return this.keys[0];
    };
    SplitExpression.prototype.firstSplitExpression = function () {
        return this.splits[this.firstSplitName()];
    };
    SplitExpression.prototype.getArgumentExpressions = function () {
        return this.mapSplits(function (name, ex) { return ex; });
    };
    SplitExpression.prototype.mapSplits = function (fn) {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var res = [];
        for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
            var k = keys_1[_i];
            var v = fn(k, splits[k]);
            if (typeof v !== 'undefined')
                res.push(v);
        }
        return res;
    };
    SplitExpression.prototype.mapSplitExpressions = function (fn) {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var ret = Object.create(null);
        for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
            var key = keys_2[_i];
            ret[key] = fn(splits[key], key);
        }
        return ret;
    };
    SplitExpression.prototype.addSplits = function (splits) {
        var newSplits = this.mapSplitExpressions(function (ex) { return ex; });
        for (var k in splits) {
            newSplits[k] = splits[k];
        }
        return this.changeSplits(newSplits);
    };
    SplitExpression.prototype.calc = function (datum) {
        var _a = this, operand = _a.operand, splits = _a.splits, dataName = _a.dataName;
        var operandValue = operand.calc(datum);
        return operandValue ? operandValue.split(splits, dataName) : null;
    };
    SplitExpression.prototype.getSQL = function (dialect) {
        var groupBys = this.mapSplits(function (name, expression) { return expression.getSQL(dialect); });
        return "GROUP BY " + groupBys.join(', ');
    };
    SplitExpression.prototype.getSelectSQL = function (dialect) {
        return this.mapSplits(function (name, expression) { return expression.getSQL(dialect) + " AS " + dialect.escapeName(name); });
    };
    SplitExpression.prototype.getGroupBySQL = function (dialect) {
        return this.mapSplits(function (name, expression) { return expression.getSQL(dialect); });
    };
    SplitExpression.prototype.getShortGroupBySQL = function () {
        return Object.keys(this.splits).map(function (d, i) { return String(i + 1); });
    };
    SplitExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.mapSplits(function (name, expression) { return expression.resolved(); }).every(Boolean);
    };
    SplitExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpleOperand = this.operand.simplify();
        var simpleSplits = this.mapSplitExpressions(function (ex) { return ex.simplify(); });
        var simpler = this.changeOperand(simpleOperand).changeSplits(simpleSplits);
        if (simpler.fullyDefined())
            return r(this.calc({}));
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler.markSimple();
    };
    SplitExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
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
        var nestDiffNext = nestDiff + 1;
        var splitsSubs = this.mapSplitExpressions(function (ex) {
            return ex._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, operandSubs.typeContext).expression;
        });
        var updatedThis = this.changeOperand(operandSubs.expression).changeSplits(splitsSubs);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
        };
    };
    SplitExpression.prototype.transformExpressions = function (fn) {
        return this.changeSplits(this.mapSplitExpressions(fn));
    };
    SplitExpression.prototype.filterFromDatum = function (datum) {
        return Expression.and(this.mapSplits(function (name, expression) {
            if (Set.isSetType(expression.type)) {
                return r(datum[name]).overlap(expression);
            }
            else {
                return expression.is(r(datum[name]));
            }
        })).simplify();
    };
    SplitExpression.prototype.hasKey = function (key) {
        return hasOwnProp(this.splits, key);
    };
    SplitExpression.prototype.isLinear = function () {
        var _a = this, splits = _a.splits, keys = _a.keys;
        for (var _i = 0, keys_3 = keys; _i < keys_3.length; _i++) {
            var k = keys_3[_i];
            var split = splits[k];
            if (Set.isSetType(split.type))
                return false;
        }
        return true;
    };
    SplitExpression.prototype.maxBucketNumber = function () {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var num = 1;
        for (var _i = 0, keys_4 = keys; _i < keys_4.length; _i++) {
            var key = keys_4[_i];
            num *= splits[key].maxPossibleSplitValues();
        }
        return num;
    };
    SplitExpression.op = "Split";
    return SplitExpression;
}(ChainableExpression));
export { SplitExpression };
Expression.applyMixins(SplitExpression, [Aggregate]);
Expression.register(SplitExpression);
