import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare type Direction = 'ascending' | 'descending';
export declare class SortExpression extends ChainableUnaryExpression {
    static DESCENDING: Direction;
    static ASCENDING: Direction;
    static DEFAULT_DIRECTION: Direction;
    static op: string;
    static fromJS(parameters: ExpressionJS): SortExpression;
    direction: Direction;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: SortExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    refName(): string;
    isNester(): boolean;
    fullyDefined(): boolean;
    changeDirection(direction: Direction): SortExpression;
    toggleDirection(): SortExpression;
    specialSimplify(): Expression;
}
