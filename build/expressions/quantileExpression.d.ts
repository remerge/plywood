import { PlywoodValue } from '../datatypes/index';
import { ChainableUnaryExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
export declare class QuantileExpression extends ChainableUnaryExpression implements Aggregate {
    static op: string;
    static fromJS(parameters: ExpressionJS): QuantileExpression;
    value: number;
    tuning: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: QuantileExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
}
