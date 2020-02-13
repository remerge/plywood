import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType } from '../types';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class SelectExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): SelectExpression;
    attributes: string[];
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: SelectExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    updateTypeContext(typeContext: DatasetFullType): DatasetFullType;
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    specialSimplify(): Expression;
}
