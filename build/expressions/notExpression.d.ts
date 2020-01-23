import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class NotExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): NotExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    protected specialSimplify(): Expression;
}
