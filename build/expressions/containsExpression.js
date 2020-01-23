import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { TransformCaseExpression } from './transformCaseExpression';
var ContainsExpression = (function (_super) {
    tslib_1.__extends(ContainsExpression, _super);
    function ContainsExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        var compare = parameters.compare;
        if (!compare) {
            compare = ContainsExpression.NORMAL;
        }
        else if (compare !== ContainsExpression.NORMAL && compare !== ContainsExpression.IGNORE_CASE) {
            throw new Error("compare must be '" + ContainsExpression.NORMAL + "' or '" + ContainsExpression.IGNORE_CASE + "'");
        }
        _this.compare = compare;
        _this._ensureOp("contains");
        _this.type = 'BOOLEAN';
        return _this;
    }
    ContainsExpression.caseIndependent = function (str) {
        return str.toUpperCase() === str.toLowerCase();
    };
    ContainsExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.compare = parameters.compare;
        return new ContainsExpression(value);
    };
    ContainsExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.compare = this.compare;
        return value;
    };
    ContainsExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.compare = this.compare;
        return js;
    };
    ContainsExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.compare === other.compare;
    };
    ContainsExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent), this.compare];
    };
    ContainsExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        var fn;
        if (this.compare === ContainsExpression.NORMAL) {
            fn = function (a, b) { return String(a).indexOf(b) > -1; };
        }
        else {
            fn = function (a, b) { return String(a).toLowerCase().indexOf(String(b).toLowerCase()) > -1; };
        }
        return Set.crossBinaryBoolean(operandValue, expressionValue, fn);
    };
    ContainsExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        var combine;
        if (this.compare === ContainsExpression.NORMAL) {
            combine = function (lhs, rhs) { return "(''+" + lhs + ").indexOf(" + rhs + ")>-1"; };
        }
        else {
            combine = function (lhs, rhs) { return "(''+" + lhs + ").toLowerCase().indexOf((''+" + rhs + ").toLowerCase())>-1"; };
        }
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, combine, operandJS[0] === '"', expressionJS[0] === '"');
    };
    ContainsExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        if (this.compare === ContainsExpression.IGNORE_CASE) {
            expressionSQL = "LOWER(" + expressionSQL + ")";
            operandSQL = "LOWER(" + operandSQL + ")";
        }
        return dialect.containsExpression(expressionSQL, operandSQL);
    };
    ContainsExpression.prototype.changeCompare = function (compare) {
        var value = this.valueOf();
        value.compare = compare;
        return new ContainsExpression(value);
    };
    ContainsExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression, compare = _a.compare;
        if (operand instanceof TransformCaseExpression && expression instanceof TransformCaseExpression) {
            var x = operand.operand, tt1 = operand.transformType;
            var y = expression.operand, tt2 = expression.transformType;
            if (tt1 === tt2) {
                return x.contains(y, ContainsExpression.IGNORE_CASE);
            }
        }
        if (compare === 'ignoreCase') {
            var expressionLiteral = expression.getLiteralValue();
            if (expressionLiteral != null &&
                ((typeof expressionLiteral === 'string' && ContainsExpression.caseIndependent(expressionLiteral)) ||
                    (expressionLiteral instanceof Set && expressionLiteral.elements.every(ContainsExpression.caseIndependent)))) {
                return this.changeCompare('normal');
            }
        }
        return this;
    };
    ContainsExpression.NORMAL = 'normal';
    ContainsExpression.IGNORE_CASE = 'ignoreCase';
    ContainsExpression.op = "Contains";
    return ContainsExpression;
}(ChainableUnaryExpression));
export { ContainsExpression };
Expression.register(ContainsExpression);
