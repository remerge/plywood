import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { PlyTypeSingleValue } from '../types';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class CustomTransformExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): CustomTransformExpression;
    custom: string;
    outputType: PlyTypeSingleValue;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: CustomTransformExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    protected _getJSChainableHelper(operandJS: string): string;
}
