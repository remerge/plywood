import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var ConcatExpression = (function (_super) {
    tslib_1.__extends(ConcatExpression, _super);
    function ConcatExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("concat");
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    ConcatExpression.fromJS = function (parameters) {
        return new ConcatExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    ConcatExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return '' + a + b; });
    };
    ConcatExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, (function (a, b) { return a + "+" + b; }), operandJS[0] === '"', expressionJS[0] === '"');
    };
    ConcatExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.concatExpression(operandSQL, expressionSQL);
    };
    ConcatExpression.prototype.isAssociative = function () {
        return true;
    };
    ConcatExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand.equals(Expression.EMPTY_STRING))
            return expression;
        if (expression.equals(Expression.EMPTY_STRING))
            return operand;
        return this;
    };
    ConcatExpression.op = "Concat";
    return ConcatExpression;
}(ChainableUnaryExpression));
export { ConcatExpression };
Expression.register(ConcatExpression);
