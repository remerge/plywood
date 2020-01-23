var Aggregate = (function () {
    function Aggregate() {
    }
    Aggregate.prototype.isAggregate = function () {
        return true;
    };
    Aggregate.prototype.isNester = function () {
        return true;
    };
    Aggregate.prototype.fullyDefined = function () {
        var expression = this.expression;
        return this.operand.isOp('literal') && (expression ? expression.resolved() : true);
    };
    return Aggregate;
}());
export { Aggregate };
