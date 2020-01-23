import * as tslib_1 from "tslib";
import { ApplyExpression } from './applyExpression';
import { ChainableExpression, Expression } from './baseExpression';
var SelectExpression = (function (_super) {
    tslib_1.__extends(SelectExpression, _super);
    function SelectExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("select");
        _this._checkOperandTypes('DATASET');
        _this.attributes = parameters.attributes;
        _this.type = 'DATASET';
        return _this;
    }
    SelectExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.attributes = parameters.attributes;
        return new SelectExpression(value);
    };
    SelectExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.attributes = this.attributes;
        return value;
    };
    SelectExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.attributes = this.attributes;
        return js;
    };
    SelectExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            String(this.attributes) === String(other.attributes);
    };
    SelectExpression.prototype._toStringParameters = function (indent) {
        return this.attributes;
    };
    SelectExpression.prototype.updateTypeContext = function (typeContext) {
        var attributes = this.attributes;
        var datasetType = typeContext.datasetType, parent = typeContext.parent;
        var newDatasetType = Object.create(null);
        for (var _i = 0, attributes_1 = attributes; _i < attributes_1.length; _i++) {
            var attr = attributes_1[_i];
            var attrType = datasetType[attr];
            if (!attrType)
                throw new Error("unknown attribute '" + attr + "' in select");
            newDatasetType[attr] = attrType;
        }
        return {
            type: 'DATASET',
            datasetType: newDatasetType,
            parent: parent
        };
    };
    SelectExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.select(this.attributes) : null;
    };
    SelectExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('can not be expressed as SQL directly');
    };
    SelectExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, attributes = _a.attributes;
        if (operand instanceof SelectExpression) {
            var x = operand.operand, attr = operand.attributes;
            return x.select(attr.filter(function (a) { return attributes.indexOf(a) !== -1; }));
        }
        else if (operand instanceof ApplyExpression) {
            var x = operand.operand, name_1 = operand.name;
            if (attributes.indexOf(name_1) === -1) {
                return this.changeOperand(x);
            }
        }
        return this;
    };
    SelectExpression.op = "Select";
    return SelectExpression;
}(ChainableExpression));
export { SelectExpression };
Expression.register(SelectExpression);
