import { ComputeFn, Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { DatasetFullType, PlyType } from '../types';
import { Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class LiteralExpression extends Expression {
    static op: string;
    static fromJS(parameters: ExpressionJS): LiteralExpression;
    value: any;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    toString(): string;
    getFn(): ComputeFn;
    calc(datum: Datum): PlywoodValue;
    getJS(datumVar: string): string;
    getSQL(dialect: SQLDialect): string;
    equals(other: LiteralExpression | undefined): boolean;
    updateTypeContext(typeContext: DatasetFullType): DatasetFullType;
    getLiteralValue(): any;
    maxPossibleSplitValues(): number;
    upgradeToType(targetType: PlyType): Expression;
}
