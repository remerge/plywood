import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
var IS_OR_OVERLAP = {
    'is': true,
    'overlap': true
};
var AndExpression = (function (_super) {
    tslib_1.__extends(AndExpression, _super);
    function AndExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("and");
        _this._checkOperandTypes('BOOLEAN');
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    AndExpression.fromJS = function (parameters) {
        return new AndExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AndExpression.merge = function (ex1, ex2) {
        if (ex1.equals(ex2))
            return ex1;
        if (!IS_OR_OVERLAP[ex1.op] || !IS_OR_OVERLAP[ex2.op])
            return null;
        var _a = ex1, lhs1 = _a.operand, rhs1 = _a.expression;
        var _b = ex2, lhs2 = _b.operand, rhs2 = _b.expression;
        if (!lhs1.equals(lhs2) || !Set.isAtomicType(lhs1.type) || !rhs1.isOp('literal') || !rhs2.isOp('literal'))
            return null;
        var intersect = Set.intersectCover(rhs1.getLiteralValue(), rhs2.getLiteralValue());
        if (intersect === null)
            return null;
        return lhs1.overlap(r(intersect)).simplify();
    };
    AndExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a && b; });
    };
    AndExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "&&" + expressionJS + ")";
    };
    AndExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + " AND " + expressionSQL + ")";
    };
    AndExpression.prototype.isCommutative = function () {
        return true;
    };
    AndExpression.prototype.isAssociative = function () {
        return true;
    };
    AndExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.FALSE))
            return Expression.FALSE;
        if (expression.equals(Expression.TRUE))
            return operand;
        if (operand instanceof AndExpression) {
            var andExpressions = operand.getExpressionList();
            for (var i = 0; i < andExpressions.length; i++) {
                var andExpression = andExpressions[i];
                var mergedExpression = AndExpression.merge(andExpression, expression);
                if (mergedExpression) {
                    andExpressions[i] = mergedExpression;
                    return Expression.and(andExpressions).simplify();
                }
            }
        }
        else {
            var mergedExpression = AndExpression.merge(operand, expression);
            if (mergedExpression)
                return mergedExpression;
        }
        return this;
    };
    AndExpression.prototype.extractFromAnd = function (matchFn) {
        if (!this.simple)
            return this.simplify().extractFromAnd(matchFn);
        var andExpressions = this.getExpressionList();
        var includedExpressions = [];
        var excludedExpressions = [];
        for (var _i = 0, andExpressions_1 = andExpressions; _i < andExpressions_1.length; _i++) {
            var ex = andExpressions_1[_i];
            if (matchFn(ex)) {
                includedExpressions.push(ex);
            }
            else {
                excludedExpressions.push(ex);
            }
        }
        return {
            extract: Expression.and(includedExpressions),
            rest: Expression.and(excludedExpressions)
        };
    };
    AndExpression.op = "And";
    return AndExpression;
}(ChainableUnaryExpression));
export { AndExpression };
Expression.register(AndExpression);
