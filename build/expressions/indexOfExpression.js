import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
var IndexOfExpression = (function (_super) {
    tslib_1.__extends(IndexOfExpression, _super);
    function IndexOfExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("indexOf");
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        _this.type = 'NUMBER';
        return _this;
    }
    IndexOfExpression.fromJS = function (parameters) {
        return new IndexOfExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    IndexOfExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.indexOf(expressionValue) : null;
    };
    IndexOfExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, (function (a, b) { return a + ".indexOf(" + b + ")"; }), operandJS[0] === '"', expressionJS[0] === '"');
    };
    IndexOfExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.indexOfExpression(operandSQL, expressionSQL);
    };
    IndexOfExpression.op = "IndexOf";
    return IndexOfExpression;
}(ChainableUnaryExpression));
export { IndexOfExpression };
Expression.register(IndexOfExpression);
