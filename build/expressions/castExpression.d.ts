import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { PlyTypeSimple } from '../types';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class CastExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): CastExpression;
    outputType: PlyTypeSimple;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: CastExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    protected specialSimplify(): Expression;
}
