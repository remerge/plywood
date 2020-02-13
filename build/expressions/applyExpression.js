import * as tslib_1 from "tslib";
import { Dataset } from '../datatypes/index';
import { indentBy } from '../helper/utils';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { ExternalExpression } from './externalExpression';
import { LiteralExpression } from './literalExpression';
import { RefExpression } from './refExpression';
var ApplyExpression = (function (_super) {
    tslib_1.__extends(ApplyExpression, _super);
    function ApplyExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.name = parameters.name;
        _this._ensureOp("apply");
        _this._checkOperandTypes('DATASET');
        _this.type = 'DATASET';
        return _this;
    }
    ApplyExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.name = parameters.name;
        return new ApplyExpression(value);
    };
    ApplyExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.name = this.name;
        return value;
    };
    ApplyExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.name = this.name;
        return js;
    };
    ApplyExpression.prototype.updateTypeContext = function (typeContext, expressionTypeContext) {
        var exprType = this.expression.type;
        typeContext.datasetType[this.name] = exprType === 'DATASET' ? expressionTypeContext : { type: exprType };
        return typeContext;
    };
    ApplyExpression.prototype._toStringParameters = function (indent) {
        var name = this.name;
        if (!RefExpression.SIMPLE_NAME_REGEXP.test(name))
            name = JSON.stringify(name);
        return [name, this.expression.toString(indent)];
    };
    ApplyExpression.prototype.toString = function (indent) {
        if (indent == null)
            return _super.prototype.toString.call(this);
        var param;
        if (this.expression.type === 'DATASET') {
            param = '\n    ' + this._toStringParameters(indent + 2).join(',\n    ') + '\n  ';
        }
        else {
            param = this._toStringParameters(indent).join(',');
        }
        var actionStr = indentBy("  .apply(" + param + ")", indent);
        return this.operand.toString(indent) + "\n" + actionStr;
    };
    ApplyExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.name === other.name;
    };
    ApplyExpression.prototype.changeName = function (name) {
        var value = this.valueOf();
        value.name = name;
        return new ApplyExpression(value);
    };
    ApplyExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (!operandValue)
            return null;
        var _a = this, name = _a.name, expression = _a.expression;
        return operandValue.apply(name, expression);
    };
    ApplyExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return expressionSQL + " AS " + dialect.escapeName(this.name);
    };
    ApplyExpression.prototype.isNester = function () {
        return true;
    };
    ApplyExpression.prototype.fullyDefined = function () {
        return false;
    };
    ApplyExpression.prototype.specialSimplify = function () {
        var _a = this, name = _a.name, operand = _a.operand, expression = _a.expression;
        if (expression instanceof RefExpression && expression.name === name && expression.nest === 0) {
            return operand;
        }
        if (expression.isAggregate() &&
            operand instanceof ApplyExpression &&
            !operand.expression.isAggregate() &&
            expression.getFreeReferences().indexOf(operand.name) === -1) {
            return this.swapWithOperand();
        }
        var dataset = operand.getLiteralValue();
        if (dataset instanceof Dataset && expression.resolved()) {
            var freeReferences = expression.getFreeReferences();
            var datum_1 = dataset.data[0];
            if (datum_1 && freeReferences.some(function (freeReference) { return datum_1[freeReference] instanceof Expression; })) {
                return this;
            }
            dataset = dataset.applyFn(name, function (d) {
                var simp = expression.resolve(d, 'null').simplify();
                if (simp instanceof ExternalExpression)
                    return simp.external;
                if (simp instanceof LiteralExpression)
                    return simp.value;
                return simp;
            }, expression.type);
            return r(dataset);
        }
        return this;
    };
    ApplyExpression.op = "Apply";
    return ApplyExpression;
}(ChainableUnaryExpression));
export { ApplyExpression };
Expression.register(ApplyExpression);
