// Texas Ranger
import _ from 'lodash'

interface Expression {
  expression: object
}

interface AST {
  body: Expression[]
}

export default class Walker {
  private ast: AST

  constructor(ast) {
    this.ast = ast
  }

  public get functions() {
    return this.buildFuncNodes(this.walk(this.baseExpression))
  }

  public get expressions() {
    return this.baseExpressions.map(expression => {
      return this.buildFuncNodes(this.walk(expression))
    })
  }

  private reduceArgs = args => {
    if (!args) {
      return []
    }

    return args.reduce(
      (acc, arg) => [...acc, ...this.getProperties(arg.properties)],
      []
    )
  }

  private walk = currentNode => {
    if (_.isEmpty(currentNode)) {
      return []
    }

    const source = currentNode.location.source
    let name
    let args
    if (currentNode.call) {
      name = currentNode.call.callee.name
      args = currentNode.call.arguments
      return [...this.walk(currentNode.argument), {name, args, source}]
    }

    name = currentNode.callee.name
    args = currentNode.arguments
    return [{name, args, source}]
  }

  private buildFuncNodes = nodes => {
    return nodes.map(({name, args, source}) => {
      return {
        name,
        arguments: this.reduceArgs(args),
        source,
      }
    })
  }

  private getProperties = props => {
    return props.map(prop => ({
      key: prop.key.name,
      value: _.get(
        prop,
        'value.value',
        _.get(prop, 'value.location.source', '')
      ),
    }))
  }

  private get baseExpressions() {
    return this.ast.body.map(({expression}) => expression)
  }

  private get baseExpression(): Expression {
    return _.get(this.ast, 'body.0.expression', {})
  }
}
