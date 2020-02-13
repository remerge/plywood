import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var CountExpression = (function (_super) {
    tslib_1.__extends(CountExpression, _super);
    function CountExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("count");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CountExpression.fromJS = function (parameters) {
        return new CountExpression(ChainableExpression.jsToValue(parameters));
    };
    CountExpression.prototype.calc = function (datum) {
        var inV = this.operand.calc(datum);
        return inV ? inV.count() : 0;
    };
    CountExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return operandSQL.indexOf(' WHERE ') === -1 ? "COUNT(*)" : "SUM(" + dialect.aggregateFilterIfNeeded(operandSQL, '1', '0') + ")";
    };
    CountExpression.op = "Count";
    return CountExpression;
}(ChainableExpression));
export { CountExpression };
Expression.applyMixins(CountExpression, [Aggregate]);
Expression.register(CountExpression);
