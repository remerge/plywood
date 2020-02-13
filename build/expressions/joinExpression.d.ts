import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType } from '../types';
import { ChainableUnaryExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { ExternalExpression } from './externalExpression';
export declare class JoinExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): JoinExpression;
    constructor(parameters: ExpressionValue);
    updateTypeContext(typeContext: DatasetFullType, expressionTypeContext: DatasetFullType): DatasetFullType;
    pushIntoExternal(): ExternalExpression | null;
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
}
