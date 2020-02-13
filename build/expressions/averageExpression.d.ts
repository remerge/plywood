import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
export declare class AverageExpression extends ChainableUnaryExpression implements Aggregate {
    static op: string;
    static fromJS(parameters: ExpressionJS): AverageExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    decomposeAverage(countEx?: Expression): Expression;
}
