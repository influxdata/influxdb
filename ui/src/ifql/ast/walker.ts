// Texas Ranger
import _ from 'lodash'

interface Expression {
  argument: object
  call: object
  location: object
  type: string
}

interface Location {
  source: string
}

interface Body {
  expression: Expression
  location: Location
  type: string
}

interface FlatExpression {
  source: string
  funcs: FuncNode[]
}

interface FuncNode {
  name: string
  arguments: any[]
  source: string
}

interface AST {
  body: Body[]
}

export default class Walker {
  private ast: AST

  constructor(ast) {
    this.ast = ast
  }

  public get functions() {
    return this.buildFuncNodes(this.walk(this.baseExpression))
  }

  public get expressions(): FlatExpression[] {
    const body = _.get(this.ast, 'body', new Array<Body>())
    return body.map(b => {
      const {location, expression} = b
      const funcs = this.buildFuncNodes(this.walk(expression))

      return {
        source: location.source,
        funcs,
      }
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

  private buildFuncNodes = (nodes): FuncNode[] => {
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

  private get baseExpression() {
    return _.get(this.ast, 'body.0.expression', {})
  }
}
