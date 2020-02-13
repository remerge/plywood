import { ComputeFn, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { External } from '../external/baseExternal';
import { DatasetFullType } from '../types';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class ExternalExpression extends Expression {
    static op: string;
    static fromJS(parameters: ExpressionJS): ExternalExpression;
    external: External;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    toString(): string;
    getFn(): ComputeFn;
    calc(datum: Datum): PlywoodValue;
    getJS(datumVar: string): string;
    getSQL(dialect: SQLDialect): string;
    equals(other: ExternalExpression | undefined): boolean;
    updateTypeContext(typeContext: DatasetFullType): DatasetFullType;
    unsuppress(): ExternalExpression;
    addExpression(expression: Expression): ExternalExpression;
    prePush(expression: ChainableUnaryExpression): ExternalExpression;
    maxPossibleSplitValues(): number;
}
