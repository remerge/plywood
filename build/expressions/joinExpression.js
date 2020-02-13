import * as tslib_1 from "tslib";
import * as hasOwnProp from 'has-own-prop';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var JoinExpression = (function (_super) {
    tslib_1.__extends(JoinExpression, _super);
    function JoinExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("join");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('DATASET');
        _this.type = 'DATASET';
        return _this;
    }
    JoinExpression.fromJS = function (parameters) {
        return new JoinExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    JoinExpression.prototype.updateTypeContext = function (typeContext, expressionTypeContext) {
        var myDatasetType = typeContext.datasetType;
        var expressionDatasetType = expressionTypeContext.datasetType;
        for (var k in expressionDatasetType) {
            typeContext.datasetType[k] = expressionDatasetType[k];
            var ft = expressionDatasetType[k];
            if (hasOwnProp(myDatasetType, k)) {
                if (myDatasetType[k].type !== ft.type) {
                    throw new Error("incompatible types of joins on " + k + " between " + myDatasetType[k].type + " and " + ft.type);
                }
            }
            else {
                myDatasetType[k] = ft;
            }
        }
        return typeContext;
    };
    JoinExpression.prototype.pushIntoExternal = function () {
        return null;
    };
    JoinExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.join(expressionValue) : null;
    };
    JoinExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        throw new Error('not possible');
    };
    JoinExpression.op = "Join";
    return JoinExpression;
}(ChainableUnaryExpression));
export { JoinExpression };
Expression.register(JoinExpression);
