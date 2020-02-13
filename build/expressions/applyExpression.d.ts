import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType } from '../types';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class ApplyExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): ApplyExpression;
    name: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    updateTypeContext(typeContext: DatasetFullType, expressionTypeContext: DatasetFullType): DatasetFullType;
    protected _toStringParameters(indent?: int): string[];
    toString(indent?: int): string;
    equals(other: ApplyExpression | undefined): boolean;
    changeName(name: string): ApplyExpression;
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    isNester(): boolean;
    fullyDefined(): boolean;
    protected specialSimplify(): Expression;
}
