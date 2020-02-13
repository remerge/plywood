import { PlywoodValue } from '../datatypes/index';
import { ChainableUnaryExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
export declare class CollectExpression extends ChainableUnaryExpression implements Aggregate {
    static op: string;
    static fromJS(parameters: ExpressionJS): CollectExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
}
