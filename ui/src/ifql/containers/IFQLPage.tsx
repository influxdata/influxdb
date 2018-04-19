import React, {PureComponent} from 'react'

import {connect} from 'react-redux'
import uuid from 'uuid'
import _ from 'lodash'

import TimeMachine, {Suggestion} from 'src/ifql/components/TimeMachine'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'
import Walker from 'src/ifql/ast/walker'
import {Func} from 'src/ifql/components/FuncArgs'
import {InputArg} from 'src/types/ifql'

import {getSuggestions, getAST} from 'src/ifql/apis'
import * as argTypes from 'src/ifql/constants/argumentTypes'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Links {
  self: string
  suggestions: string
  ast: string
}

interface Props {
  links: Links
}

interface State {
  suggestions: Suggestion[]
  expressions: Expression[]
  ast: object
  script: string
}

interface Expression {
  id: string
  funcs: Func[]
  source: string
}

@ErrorHandling
export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      suggestions: [],
      expressions: [],
      ast: null,
      script:
        'from(db: "telegraf")\n\t|> filter() \n\t|> range(start: -15m)\n\nfrom(db: "telegraf")\n\t|> filter() \n\t|> range(start: -15m)\n\n',
    }
  }

  public async componentDidMount() {
    const {links} = this.props

    try {
      const suggestions = await getSuggestions(links.suggestions)
      this.setState({suggestions})
    } catch (error) {
      console.error('Could not get function suggestions: ', error)
    }

    this.getASTResponse(this.state.script)
  }

  public render() {
    const {suggestions, script} = this.state

    return (
      <KeyboardShortcuts onControlEnter={this.handleSubmitScript}>
        <div className="page hosts-list-page">
          <div className="page-header">
            <div className="page-header__container">
              <div className="page-header__left">
                <h1 className="page-header__title">Time Machine</h1>
              </div>
            </div>
          </div>
          <div className="page-contents">
            <div className="container-fluid">
              <TimeMachine
                script={script}
                expressions={this.state.expressions}
                suggestions={suggestions}
                onAddNode={this.handleAddNode}
                onChangeArg={this.handleChangeArg}
                onSubmitScript={this.handleSubmitScript}
                onChangeScript={this.handleChangeScript}
                onDeleteFuncNode={this.handleDeleteFuncNode}
                onGenerateScript={this.handleGenerateScript}
              />
            </div>
          </div>
        </div>
      </KeyboardShortcuts>
    )
  }

  private handleSubmitScript = () => {
    this.getASTResponse(this.state.script)
  }

  private handleGenerateScript = (): void => {
    this.getASTResponse(this.expressionsToScript)
  }

  private handleChangeArg = ({
    key,
    value,
    generate,
    funcID,
    expressionID,
  }: InputArg): void => {
    const expressions = this.state.expressions.map(expression => {
      if (expression.id !== expressionID) {
        return expression
      }

      const funcs = expression.funcs.map(f => {
        if (f.id !== funcID) {
          return f
        }

        const args = f.args.map(a => {
          if (a.key === key) {
            return {...a, value}
          }

          return a
        })

        return {...f, args}
      })

      return {...expression, funcs}
    })

    this.setState({expressions}, () => {
      if (generate) {
        this.handleGenerateScript()
      }
    })
  }

  private get expressionsToScript(): string {
    return this.state.expressions.reduce((acc, expression) => {
      return `${acc + this.funcsToScript(expression.funcs)}\n\n`
    }, '')
  }

  private funcsToScript(funcs): string {
    return funcs
      .map(func => `${func.name}(${this.argsToScript(func.args)})`)
      .join('\n\t|> ')
  }

  private argsToScript(args): string {
    const withValues = args.filter(arg => arg.value || arg.value === false)

    return withValues
      .map(({key, value, type}) => {
        if (type === argTypes.STRING) {
          return `${key}: "${value}"`
        }

        if (type === argTypes.ARRAY) {
          return `${key}: [${value}]`
        }

        return `${key}: ${value}`
      })
      .join(', ')
  }

  private handleChangeScript = (script: string): void => {
    this.setState({script})
  }

  private handleAddNode = (name: string, expressionID: string): void => {
    const script = this.state.expressions.reduce((acc, expression) => {
      if (expression.id === expressionID) {
        const {funcs} = expression
        return `${acc}${this.funcsToScript(funcs)}\n\t|> ${name}()\n\n`
      }

      return acc + expression.source
    }, '')

    this.getASTResponse(script)
  }

  private handleDeleteFuncNode = (
    funcID: string,
    expressionID: string
  ): void => {
    // TODO: export this and test functionality
    const script = this.state.expressions
      .map((expression, expressionIndex) => {
        if (expression.id !== expressionID) {
          return expression.source
        }

        const funcs = expression.funcs.filter(f => f.id !== funcID)
        const source = funcs.reduce((acc, f, i) => {
          if (i === 0) {
            return `${f.source}`
          }

          return `${acc}\n\t${f.source}`
        }, '')

        const isLast = expressionIndex === this.state.expressions.length - 1
        if (isLast) {
          return `${source}`
        }

        return `${source}\n\n`
      })
      .join('')

    this.getASTResponse(script)
  }

  private expressions = (ast, suggestions): Expression[] => {
    if (!ast) {
      return []
    }

    const walker = new Walker(ast)

    const expressions = walker.expressions.map(({funcs, source}) => {
      const id = uuid.v4()
      return {
        id,
        funcs: this.functions(funcs, suggestions),
        source,
      }
    })

    return expressions
  }

  private functions = (funcs, suggestions): Func[] => {
    const functions = funcs.map(func => {
      const {params, name} = suggestions.find(f => f.name === func.name)

      const args = Object.entries(params).map(([key, type]) => {
        const value = _.get(
          func.arguments.find(arg => arg.key === key),
          'value',
          ''
        )

        return {
          key,
          value,
          type,
        }
      })

      return {
        id: uuid.v4(),
        source: func.source,
        name,
        args,
      }
    })

    return functions
  }

  private getASTResponse = async (script: string) => {
    const {links} = this.props

    try {
      const ast = await getAST({url: links.ast, body: script})
      const expressions = this.expressions(ast, this.state.suggestions)
      this.setState({ast, script, expressions})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
