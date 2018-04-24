import React, {PureComponent} from 'react'

import {connect} from 'react-redux'

import TimeMachine from 'src/ifql/components/TimeMachine'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'
import {Suggestion, FlatBody} from 'src/types/ifql'
import {InputArg, Handlers} from 'src/types/ifql'

import {bodyNodes} from 'src/ifql/helpers'
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

interface Body extends FlatBody {
  id: string
}

interface State {
  body: Body[]
  ast: object
  script: string
  suggestions: Suggestion[]
}

export const IFQLContext = React.createContext()

@ErrorHandling
export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      body: [],
      ast: null,
      suggestions: [],
      script:
        'foo = from(db: "telegraf")\n\t|> filter() \n\t|> range(start: -15m)\n\nfrom(db: "telegraf")\n\t|> filter() \n\t|> range(start: -15m)\n\n',
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
      <IFQLContext.Provider value={this.handlers}>
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
                  body={this.state.body}
                  suggestions={suggestions}
                  onSubmitScript={this.handleSubmitScript}
                  onChangeScript={this.handleChangeScript}
                />
              </div>
            </div>
          </div>
        </KeyboardShortcuts>
      </IFQLContext.Provider>
    )
  }

  private get handlers(): Handlers {
    return {
      onAddNode: this.handleAddNode,
      onChangeArg: this.handleChangeArg,
      onSubmitScript: this.handleSubmitScript,
      onChangeScript: this.handleChangeScript,
      onDeleteFuncNode: this.handleDeleteFuncNode,
      onGenerateScript: this.handleGenerateScript,
    }
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
    bodyID,
  }: InputArg): void => {
    const body = this.state.body.map(expression => {
      if (expression.id !== bodyID) {
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

    this.setState({body}, () => {
      if (generate) {
        this.handleGenerateScript()
      }
    })
  }

  private get expressionsToScript(): string {
    return this.state.body.reduce((acc, expression) => {
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

  private handleAddNode = (name: string, bodyID: string): void => {
    const script = this.state.body.reduce((acc, expression) => {
      if (expression.id === bodyID) {
        const {funcs} = expression
        return `${acc}${this.funcsToScript(funcs)}\n\t|> ${name}()\n\n`
      }

      return acc + expression.source
    }, '')

    this.getASTResponse(script)
  }

  private handleDeleteFuncNode = (funcID: string, bodyID: string): void => {
    // TODO: export this and test functionality
    const script = this.state.body
      .map((expression, expressionIndex) => {
        if (expression.id !== bodyID) {
          return expression.source
        }

        const funcs = expression.funcs.filter(f => f.id !== funcID)
        const source = funcs.reduce((acc, f, i) => {
          if (i === 0) {
            return `${f.source}`
          }

          return `${acc}\n\t${f.source}`
        }, '')

        const isLast = expressionIndex === this.state.body.length - 1
        if (isLast) {
          return `${source}`
        }

        return `${source}\n\n`
      })
      .join('')

    this.getASTResponse(script)
  }

  private getASTResponse = async (script: string) => {
    const {links} = this.props

    try {
      const ast = await getAST({url: links.ast, body: script})
      const body = bodyNodes(ast, this.state.suggestions)
      this.setState({ast, script, body})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
