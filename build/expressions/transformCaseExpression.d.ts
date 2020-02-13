import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { CaseType, ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class TransformCaseExpression extends ChainableExpression {
    static UPPER_CASE: string;
    static LOWER_CASE: string;
    static op: string;
    static fromJS(parameters: ExpressionJS): TransformCaseExpression;
    transformType: CaseType;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: TransformCaseExpression | undefined): boolean;
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    specialSimplify(): Expression;
}
