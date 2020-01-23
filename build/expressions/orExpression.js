import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
var IS_OR_OVERLAP = {
    'is': true,
    'overlap': true
};
var OrExpression = (function (_super) {
    tslib_1.__extends(OrExpression, _super);
    function OrExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("or");
        _this._checkOperandTypes('BOOLEAN');
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    OrExpression.fromJS = function (parameters) {
        return new OrExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    OrExpression.merge = function (ex1, ex2) {
        if (ex1.equals(ex2))
            return ex1;
        if (!IS_OR_OVERLAP[ex1.op] || !IS_OR_OVERLAP[ex2.op])
            return null;
        var _a = ex1, lhs1 = _a.operand, rhs1 = _a.expression;
        var _b = ex2, lhs2 = _b.operand, rhs2 = _b.expression;
        if (!lhs1.equals(lhs2) || !rhs1.isOp('literal') || !rhs2.isOp('literal'))
            return null;
        var union = Set.unionCover(rhs1.getLiteralValue(), rhs2.getLiteralValue());
        if (union === null)
            return null;
        return lhs1.overlap(r(union)).simplify();
    };
    OrExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue || expressionValue;
    };
    OrExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "||" + expressionJS + ")";
    };
    OrExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + " OR " + expressionSQL + ")";
    };
    OrExpression.prototype.isCommutative = function () {
        return true;
    };
    OrExpression.prototype.isAssociative = function () {
        return true;
    };
    OrExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.TRUE))
            return Expression.TRUE;
        if (expression.equals(Expression.FALSE))
            return operand;
        if (operand instanceof OrExpression) {
            var orExpressions = operand.getExpressionList();
            for (var i = 0; i < orExpressions.length; i++) {
                var orExpression = orExpressions[i];
                var mergedExpression = OrExpression.merge(orExpression, expression);
                if (mergedExpression) {
                    orExpressions[i] = mergedExpression;
                    return Expression.or(orExpressions).simplify();
                }
            }
        }
        else {
            var mergedExpression = OrExpression.merge(operand, expression);
            if (mergedExpression)
                return mergedExpression;
        }
        return this;
    };
    OrExpression.op = "Or";
    return OrExpression;
}(ChainableUnaryExpression));
export { OrExpression };
Expression.register(OrExpression);
