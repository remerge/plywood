import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
var ThenExpression = (function (_super) {
    tslib_1.__extends(ThenExpression, _super);
    function ThenExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("then");
        _this._checkOperandTypes('BOOLEAN');
        _this.type = _this.expression.type;
        return _this;
    }
    ThenExpression.fromJS = function (parameters) {
        return new ThenExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    ThenExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? expressionValue : null;
    };
    ThenExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "((_=" + operandJS + "),(_?" + expressionJS + ":null))";
    };
    ThenExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.ifThenElseExpression(operandSQL, expressionSQL);
    };
    ThenExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.NULL))
            return operand;
        if (operand.equals(Expression.NULL))
            return Expression.NULL;
        if (operand.equals(Expression.FALSE))
            return Expression.NULL;
        if (operand.equals(Expression.TRUE))
            return expression;
        return this;
    };
    ThenExpression.op = "Then";
    return ThenExpression;
}(ChainableUnaryExpression));
export { ThenExpression };
Expression.register(ThenExpression);
