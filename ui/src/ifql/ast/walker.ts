// Texas Ranger
import {get} from 'lodash'

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
    let name
    let args
    if (currentNode.call) {
      name = currentNode.call.callee.name
      args = currentNode.call.arguments
      return [...this.walk(currentNode.argument), {name, args}]
    }

    name = currentNode.callee.name
    args = currentNode.arguments
    return [{name, args}]
  }

  private buildFuncNodes = nodes => {
    return nodes.map(node => {
      return {
        name: node.name,
        arguments: this.reduceArgs(node.args),
      }
    })
  }

  private getProperties = props => {
    return props.map(prop => ({
      key: prop.key.name,
      value: get(prop, 'value.value', get(prop, 'value.location.source', '')),
    }))
  }

  private get baseExpression() {
    return this.ast.body[0].expression
  }
}
