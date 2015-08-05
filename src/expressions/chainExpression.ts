module Plywood {
  export class ChainExpression extends Expression {
    static fromJS(parameters: ExpressionJS): ChainExpression {
      var value: ExpressionValue = {
        op: parameters.op
      };
      value.expression = Expression.fromJS(parameters.expression);
      value.actions = parameters.actions.map(Action.fromJS);
      return new ChainExpression(value);
    }

    public expression: Expression;
    public actions: Action[];

    constructor(parameters: ExpressionValue) {
      super(parameters, dummyObject);
      var expression = parameters.expression;
      this.expression = expression;
      var actions = parameters.actions;
      this.actions = actions;
      this._ensureOp('chain');

      var type = expression.type;
      for (var action of actions) {
        type = action.getOutputType(type);
      }
      this.type = type;
    }

    public valueOf(): ExpressionValue {
      var value = super.valueOf();
      value.expression = this.expression;
      value.actions = this.actions;
      return value;
    }

    public toJS(): ExpressionJS {
      var js = super.toJS();
      js.expression = this.expression.toJS();
      js.actions = this.actions.map(action => action.toJS());
      return js;
    }

    public toString(): string {
      return [this.expression.toString()].concat(this.actions.map(action => action.toString())).join('\n  .');
    }

    public equals(other: ChainExpression): boolean {
      return super.equals(other) &&
             this.expression.equals(other.expression) &&
             higherArraysEqual(this.actions, other.actions);
    }

    public expressionCount(): int {
      var expressionCount = 1 + this.expression.expressionCount();
      var actions = this.actions;
      for (let action of actions) {
        expressionCount += action.expressionCount();
      }
      return expressionCount;
    }

    public getFn(): ComputeFn {
      var fn = this.expression.getFn();
      var actions = this.actions;
      for (let action of actions) {
        fn = action.getFn(fn);
      }
      return fn;
    }

    /*
    public getFn(): ComputeFn {
      var ex = this;
      var expression = this.expression;
      var actions = this.actions;
      return (d: Datum, context: Datum) => {
        var input = expression.getFn()(null);

        for (let action of actions) {
          input = action.getFn(() => input)(null);
        }

        return input;
      };
    }
    */

    public getJS(datumVar: string): string {
      throw new Error("can not call getJS on actions");
    }

    public getSQL(dialect: SQLDialect, minimal: boolean = false): string {
      throw new Error("can not call getSQL on actions");
    }

    public simplify(): Expression {
      if (this.simple) return this;

      var simpleExpression = this.expression.simplify();
      if (simpleExpression instanceof ChainExpression) {
        return new ChainExpression({
          expression: simpleExpression.expression,
          actions: simpleExpression.actions.concat(this.actions)
        }).simplify();
      }

      // Simplify all actions
      var actions = this.actions;
      var simpleActions: Action[] = [];
      for (let action of actions) {
        let actionSimplification = action.simplify();
        if (actionSimplification) {
          switch (actionSimplification.simplification) {
            case Simplification.Replace:
              simpleActions = simpleActions.concat(actionSimplification.actions);
              break;

            case Simplification.Wipe:
              simpleActions = [];
              simpleExpression = actionSimplification.expression.simplify();
              break;

            default: // Simplification.Remove
              break;
          }
        } else {
          simpleActions.push(action);
        }
      }

      // In case of literal fold accordingly
      while (simpleExpression instanceof LiteralExpression && simpleActions.length) {
        let foldedExpression = simpleActions[0].foldLiteral(<LiteralExpression>simpleExpression);
        if (!foldedExpression) break;
        simpleActions.shift();
        simpleExpression = foldedExpression.simplify();
      }

      // ToDo: try to merge actions here

      if (simpleExpression instanceof LiteralExpression && simpleExpression.type === 'DATASET' && simpleActions.length) {
        var dataset: Dataset = (<LiteralExpression>simpleExpression).value;
        var externalAction = simpleActions[0];
        var externalExpression = externalAction.expression;
        if (dataset.basis() && externalAction.action === 'apply' && externalExpression instanceof ExternalExpression) {
          simpleExpression = externalExpression.makeTotal();
          simpleActions.shift();
        }
      }

      if (simpleExpression instanceof ExternalExpression) {
        while (simpleActions.length) {
          let newSimpleExpression = (<ExternalExpression>simpleExpression).addAction(simpleActions[0]);
          if (!newSimpleExpression) break;
          simpleExpression = newSimpleExpression;
          simpleActions.shift();
        }
      }



      /*
      function isRemoteSimpleApply(action: Action): boolean {
        return action instanceof ApplyAction && action.expression.hasRemote() && action.expression.type !== 'DATASET';
      }

      // These are actions on a remote dataset
      var externals = this.getExternals();
      var external: External;
      var digestedOperand = simpleExpression;
      if (externals.length && (digestedOperand instanceof LiteralExpression || digestedOperand instanceof JoinExpression)) {
        external = externals[0];
        if (digestedOperand instanceof LiteralExpression && !digestedOperand.isRemote() && simpleActions.some(isRemoteSimpleApply)) {
          if (externals.length === 1) {
            digestedOperand = new LiteralExpression({
              op: 'literal',
              value: external.makeTotal()
            });
          } else {
            throw new Error('not done yet')
          }
        }

        var undigestedActions: Action[] = [];
        for (var i = 0; i < simpleActions.length; i++) {
          var action: Action = simpleActions[i];
          var digest = external.digest(digestedOperand, action);
          if (digest) {
            digestedOperand = digest.expression;
            if (digest.undigested) undigestedActions.push(digest.undigested);

          } else {
            undigestedActions.push(action);
          }
        }
        if (simpleExpression !== digestedOperand) {
          simpleExpression = digestedOperand;
          simpleActions = defsToAddBack.concat(undigestedActions);
        }
      }
      */

      if (simpleActions.length === 0) return simpleExpression;

      var simpleValue = this.valueOf();
      simpleValue.expression = simpleExpression;
      simpleValue.actions = simpleActions;
      simpleValue.simple = true;
      return new ChainExpression(simpleValue);
    }

    public _everyHelper(iter: BooleanExpressionIterator, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): boolean {
      var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
      if (pass != null) {
        return pass;
      } else {
        indexer.index++;
      }
      depth++;

      var expression = this.expression;
      if (!expression._everyHelper(iter, thisArg, indexer, depth, nestDiff)) return false;
      var actionNestDiff = nestDiff + (expression.type === 'DATASET' ? 1 : 0);

      var actions = this.actions;
      var every: boolean = true;
      for (let action of actions) {
        if (every) {
          every = action._everyHelper(iter, thisArg, indexer, depth, actionNestDiff);
          actionNestDiff += action.contextDiff();
        } else {
          indexer.index += action.expressionCount();
        }
      }
      return every;
    }

    public _substituteHelper(substitutionFn: SubstitutionFn, thisArg: any, indexer: Indexer, depth: int, nestDiff: int): Expression {
      var sub = substitutionFn.call(thisArg, this, indexer.index, depth, nestDiff);
      if (sub) {
        indexer.index += this.expressionCount();
        return sub;
      } else {
        indexer.index++;
      }
      depth++;

      var expression = this.expression;
      var subExpression = expression._substituteHelper(substitutionFn, thisArg, indexer, depth, nestDiff);
      var actionNestDiff = nestDiff + (expression.type === 'DATASET' ? 1 : 0);

      var actions = this.actions;
      var subActions = actions.map(action => {
        var subbedAction = action._substituteHelper(substitutionFn, thisArg, indexer, depth, actionNestDiff);
        actionNestDiff += action.contextDiff();
        return subbedAction;
      });
      if (expression === subExpression && arraysEqual(actions, subActions)) return this;

      var value = this.valueOf();
      value.expression = subExpression;
      value.actions = subActions;
      delete value.simple;
      return new ChainExpression(value);
    }

    public _performAction(action: Action): ChainExpression {
      return new ChainExpression({
        expression: this.expression,
        actions: this.actions.concat(action)
      });
    }

    public _fillRefSubstitutions(typeContext: FullType, indexer: Indexer, alterations: Alterations): FullType {
      indexer.index++;

      // Some explanation of what is going on here is in order as this is the heart of the variable resolution code
      // The _fillRefSubstitutions function is chained across all the expressions.
      // If an expression returns a DATASET type it is treated as the new context otherwise the original context is
      // used for the next expression (currentContext)
      var currentContext = typeContext;
      var outputContext = this.expression._fillRefSubstitutions(currentContext, indexer, alterations);
      currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;

      var actions = this.actions;
      for (let action of actions) {
        outputContext = action._fillRefSubstitutions(currentContext, indexer, alterations);
        currentContext = outputContext.type === 'DATASET' ? outputContext : typeContext;
      }

      return outputContext;
    }

    public actionize(containingAction: string): Action[] {
      var actions = this.actions;

      var k = actions.length - 1;
      for (; k >= 0; k--) {
        if (actions[k].action !== containingAction) break;
      }
      k++; // k now represents the number of actions that remain in the chain
      if (k === actions.length) return null; // nothing to do

      var newExpression: Expression;
      if (k === 0) {
        newExpression = this.expression;
      } else {
        var value = this.valueOf();
        value.actions = actions.slice(0, k);
        newExpression = new ChainExpression(value);
      }

      return [
        new Action.classMap[containingAction]({
          expression: newExpression
        })
      ].concat(actions.slice(k));
    }

    public getExpressionPattern(actionType: string): Expression[] {
      var actions = this.actionize(actionType);
      if (actions.length < 2) return null;
      return actions.map((action) => action.expression);
    }

    public _collectBindSpecs(bindSpecs: BindSpec[], selectionDepth: Lookup<number>, depth: number, applyName: string, data: string, key: string): void {
      var expression = this.expression;
      var actions = this.actions;
      switch (expression.type) {
        case 'DATASET':
          var nextData: string = null;
          var nextKey: string = null;
          for (let action of actions) {
            if (action instanceof SplitAction) {
              nextData = applyName;
              nextKey = action.name;
              depth++;
            } else if (action instanceof ApplyAction) {
              action.expression._collectBindSpecs(bindSpecs, selectionDepth, depth, action.name, nextData, nextKey);
            }
          }
          break;

        case 'MARK':
          var selectionInput = (<RefExpression>expression).name;
          for (let action of actions) {
            if (action instanceof AttachAction) {
              var bindSpec: BindSpec = {
                selectionInput,
                selector: action.selector,
                selectionName: applyName
              };
              if (!hasOwnProperty(selectionDepth, selectionInput)) throw new Error('something terrible has happened');
              if (data && depth > selectionDepth[selectionInput]) {
                bindSpec.data = data;
                bindSpec.key = key;
              }
              fillMethods(action.prop, bindSpec);
              bindSpecs.push(bindSpec);
              selectionDepth[applyName] = depth;
            } else {
              throw new Error('unknown action ' + action.action);
            }
          }

          break;
      }
    }

    public _computeResolved(): Q.Promise<Dataset> {
      var actions = this.actions;

      /*
      function execAction(i: int) {
        return (dataset: Dataset): Dataset | Q.Promise<Dataset> => {
          var action = actions[i];
          var actionExpression = action.expression;

          if (action instanceof FilterAction) {
            return dataset.filter(action.expression.getFn());

          } else if (action instanceof ApplyAction) {
            if (actionExpression instanceof ChainExpression) {
              return dataset.applyPromise(action.name, (d: Datum) => {
                return actionExpression.resolve(d).simplify()._computeResolved();
              });
            } else {
              return dataset.apply(action.name, actionExpression.getFn());
            }

          } else if (action instanceof SortAction) {
            return dataset.sort(actionExpression.getFn(), action.direction);

          } else if (action instanceof LimitAction) {
            return dataset.limit(action.limit);

          }
        }
      }
      */

      var promise = this.expression._computeResolved();
      for (var i = 0; i < actions.length; i++) {
        //promise = promise.then(execAction(i));
      }
      return promise;
    }

    public separateViaAnd(refName: string): Separation {
      if (typeof refName !== 'string') throw new Error('must have refName');
      /*
      //if (!this.simple) return this.simplify().separateViaAnd(refName);

      var includedExpressions: Expression[] = [];
      var excludedExpressions: Expression[] = [];
      var expressions = this.expressions;
      for (let operand of operands) {
        var sep = operand.separateViaAnd(refName);
        if (sep === null) return null;
        includedExpressions.push(sep.included);
        excludedExpressions.push(sep.excluded);
      }

      return {
        included: new AndExpression({ op: 'and', operands: includedExpressions }).simplify(),
        excluded: new AndExpression({ op: 'and', operands: excludedExpressions }).simplify()
      };
      */
      return null;
    }
  }

  Expression.register(ChainExpression);
}