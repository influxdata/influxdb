// Texas Ranger
import _ from 'lodash'
import {
  Func,
  FlatBody,
  BinaryExpressionNode,
  MemberExpressionNode,
} from 'src/types/flux'

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
  type: string
  source: string
  funcs: Func[]
}

interface AST {
  body: Body[]
}

type InOrderNode = BinaryExpressionNode | MemberExpressionNode

export default class Walker {
  private ast: AST

  constructor(ast) {
    this.ast = ast
  }

  public get functions() {
    return this.buildFuncNodes(this.walk(this.baseExpression))
  }

  public get body(): FlatBody[] {
    const body = _.get(this.ast, 'body', new Array<Body>())
    return body.map(b => {
      if (b.type.includes('Expression')) {
        return this.expression(b.expression, b.location)
      } else if (b.type.includes('Variable')) {
        return this.variable(b)
      }
    })
  }

  public get inOrderExpression(): InOrderNode[] {
    const tree = _.get(this.ast, 'body.0.expression.body', new Array<Body>())
    return this.inOrder(tree)
  }

  private hasParen = (parent, child): boolean => {
    if (parent.operator && parent.operator.toLowerCase() === 'and') {
      // for mathematical operations:
      // if parent and child have operators
      //   // return child precedence < parent precedence
      // return false
      if (
        child.type === 'LogicalExpression' &&
        child.operator.toLowerCase() === 'or'
      ) {
        return true
      }
    }

    return false
  }

  private inOrder = (node, paren = false): InOrderNode[] => {
    let results = []
    if (node) {
      if (paren) {
        results = [...results, {source: '(', type: 'OpenParen'}]
      }

      const isLeftParen = this.hasParen(node, node.left)

      results = [...results, ...this.inOrder(node.left, isLeftParen)]

      if (
        node.type === 'MemberExpression' ||
        node.type === 'ObjectExpression'
      ) {
        const {location, object, property} = node
        const {name, type, value} = property
        const {source} = location

        results = [
          ...results,
          {
            source,
            object: {name: object.name, type: object.type},
            property: {name: name || value, type},
            type: node.type,
          },
        ]
      }

      if (node.operator) {
        results = [...results, {type: 'Operator', source: node.operator}]
      }

      if (node.name) {
        results = [...results, {type: node.type, source: node.location.source}]
      }

      if (node.value) {
        results = [...results, {type: node.type, source: node.location.source}]
      }

      const isRightParen = this.hasParen(node, node.right)

      results = [...results, ...this.inOrder(node.right, isRightParen)]

      if (paren) {
        results = [...results, {source: ')', type: 'CloseParen'}]
      }
    }

    return results
  }

  private variable(variable) {
    const {location} = variable
    const declarations = variable.declarations.map(d => {
      const {init} = d
      const {name} = d.id
      const {type, value} = init

      if (type === 'ArrowFunctionExpression') {
        return {
          name,
          type,
          params: this.params(init.params),
          body: this.inOrder(init.body),
          source: init.location.source,
        }
      }

      if (type.includes('Expression')) {
        const {source, funcs} = this.expression(d.init, location)
        return {name, type, source, funcs}
      }

      return {name, type, value}
    })

    return {source: location.source, declarations, type: variable.type}
  }

  private params = params => {
    return params.map(p => {
      return {source: p.key.location.source, type: p.type}
    })
  }

  // returns an in order flattening of a binary expression
  private expression(expression, location): FlatExpression {
    const funcs = this.buildFuncNodes(this.walk(expression))

    return {
      type: expression.type,
      source: location.source,
      funcs,
    }
  }

  public get expressions(): FlatExpression[] {
    const body = _.get(this.ast, 'body', new Array<Body>())
    return body.map(b => {
      const {location, expression} = b
      const funcs = this.buildFuncNodes(this.walk(expression))

      return {
        type: expression.type,
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

    if (currentNode.type === 'Identifier') {
      name = currentNode.name
      return [{name, source}]
    }

    name = currentNode.callee.name
    args = currentNode.arguments
    return [{name, args, source}]
  }

  private buildFuncNodes = (nodes): Func[] => {
    return nodes.map(({name, args, source}) => {
      return {
        name,
        args: this.reduceArgs(args),
        source,
      }
    })
  }

  private constructObject(value) {
    const propArray = _.get(value, 'properties', [])
    const valueObj = propArray.reduce((acc, p) => {
      return {...acc, [p.key.name]: p.value.name}
    }, {})
    return valueObj
  }

  private constructArray(value) {
    const elementsArray = _.get(value, 'elements', [])
    const valueArray = elementsArray.reduce((acc, e) => {
      return [...acc, e.value]
    }, [])
    return valueArray
  }

  private getProperties = props => {
    return props.map(prop => {
      const key = prop.key.name
      let value
      if (_.get(prop, 'value.type', '') === 'ObjectExpression') {
        value = this.constructObject(prop.value)
      } else if (_.get(prop, 'value.type', '') === 'ArrayExpression') {
        value = this.constructArray(prop.value)
      } else {
        value = _.get(
          prop,
          'value.value',
          _.get(prop, 'value.location.source', '')
        )
      }
      return {
        key,
        value,
      }
    })
  }

  private get baseExpression() {
    return _.get(this.ast, 'body.0.expression', {})
  }
}
