import React, {PureComponent} from 'react'

import {connect} from 'react-redux'
import _ from 'lodash'

import TimeMachine from 'src/ifql/components/TimeMachine'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'
import {Suggestion, FlatBody, Links} from 'src/types/ifql'
import {InputArg, Handlers, DeleteFuncNodeArgs, Func} from 'src/types/ifql'

import {bodyNodes} from 'src/ifql/helpers'
import {getSuggestions, getAST, getTimeSeries} from 'src/ifql/apis'
import * as argTypes from 'src/ifql/constants/argumentTypes'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Status {
  type: string
  text: string
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
  data: string
  suggestions: Suggestion[]
  status: Status
}

export const IFQLContext = React.createContext()

@ErrorHandling
export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      body: [],
      ast: null,
      data: 'Hit "Get Data!" or Ctrl + Enter to run your script',
      suggestions: [],
      script: `"fil = (r) => r._measurement == \"cpu\"\ntele = from(db: \"telegraf\") \n\t\t|> filter(fn: fil)\n        |> range(start: -1m)\n        |> sum()"`,
      status: {
        type: 'none',
        text: '',
      },
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
    const {suggestions, script, data, body, status} = this.state

    return (
      <IFQLContext.Provider value={this.handlers}>
        <KeyboardShortcuts onControlEnter={this.getTimeSeries}>
          <div className="page hosts-list-page">
            <div className="page-header full-width">
              <div className="page-header__container">
                <div className="page-header__left">
                  <h1 className="page-header__title">Time Machine</h1>
                </div>
                <div className="page-header__right">
                  <button
                    className="btn btn-sm btn-primary"
                    onClick={this.getTimeSeries}
                  >
                    Get Data!
                  </button>
                </div>
              </div>
            </div>
            <TimeMachine
              data={data}
              body={body}
              script={script}
              status={status}
              suggestions={suggestions}
              onChangeScript={this.handleChangeScript}
              onSubmitScript={this.handleSubmitScript}
            />
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
    this.getASTResponse(this.bodyToScript)
  }

  private handleChangeArg = ({
    key,
    value,
    generate,
    funcID,
    declarationID = '',
    bodyID,
  }: InputArg): void => {
    const body = this.state.body.map(b => {
      if (b.id !== bodyID) {
        return b
      }

      if (declarationID) {
        const declarations = b.declarations.map(d => {
          if (d.id !== declarationID) {
            return d
          }

          const functions = this.editFuncArgs({
            funcs: d.funcs,
            funcID,
            key,
            value,
          })

          return {...d, funcs: functions}
        })

        return {...b, declarations}
      }

      const funcs = this.editFuncArgs({
        funcs: b.funcs,
        funcID,
        key,
        value,
      })

      return {...b, funcs}
    })

    this.setState({body}, () => {
      if (generate) {
        this.handleGenerateScript()
      }
    })
  }

  private editFuncArgs = ({funcs, funcID, key, value}): Func[] => {
    return funcs.map(f => {
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
  }

  private get bodyToScript(): string {
    return this.state.body.reduce((acc, b) => {
      if (b.declarations.length) {
        const declaration = _.get(b, 'declarations.0', false)
        if (!declaration) {
          return acc
        }

        if (!declaration.funcs) {
          return `${acc}${b.source}\n\n`
        }

        return `${acc}${declaration.name} = ${this.funcsToScript(
          declaration.funcs
        )}\n\n`
      }

      return `${acc}${this.funcsToScript(b.funcs)}\n\n`
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

  private handleAddNode = (
    name: string,
    bodyID: string,
    declarationID: string
  ): void => {
    const script = this.state.body.reduce((acc, body) => {
      const {id, source, funcs} = body

      if (id === bodyID) {
        const declaration = body.declarations.find(d => d.id === declarationID)
        if (declaration) {
          return `${acc}${declaration.name} = ${this.appendFunc(
            declaration.funcs,
            name
          )}`
        }

        return `${acc}${this.appendFunc(funcs, name)}`
      }

      return `${acc}${this.formatSource(source)}`
    }, '')

    this.getASTResponse(script)
  }

  private appendFunc = (funcs, name): string => {
    return `${this.funcsToScript(funcs)}\n\t|> ${name}()\n\n`
  }

  private handleDeleteFuncNode = (ids: DeleteFuncNodeArgs): void => {
    const {funcID, declarationID = '', bodyID} = ids

    const script = this.state.body
      .map((body, bodyIndex) => {
        if (body.id !== bodyID) {
          return this.formatSource(body.source)
        }

        const isLast = bodyIndex === this.state.body.length - 1

        if (declarationID) {
          const declaration = body.declarations.find(
            d => d.id === declarationID
          )

          if (!declaration) {
            return
          }

          const functions = declaration.funcs.filter(f => f.id !== funcID)
          const s = this.funcsToSource(functions)
          return `${declaration.name} = ${this.formatLastSource(s, isLast)}`
        }

        const funcs = body.funcs.filter(f => f.id !== funcID)
        const source = this.funcsToSource(funcs)
        return this.formatLastSource(source, isLast)
      })
      .join('')

    this.getASTResponse(script)
  }

  private formatSource = (source: string): string => {
    // currently a bug in the AST which does not add newlines to literal variable assignment bodies
    if (!source.match(/\n\n/)) {
      return `${source}\n\n`
    }

    return `${source}`
  }

  // formats the last line of a body string to include two new lines
  private formatLastSource = (source: string, isLast: boolean): string => {
    if (isLast) {
      return `${source}`
    }

    // currently a bug in the AST which does not add newlines to literal variable assignment bodies
    if (!source.match(/\n\n/)) {
      return `${source}\n\n`
    }

    return `${source}\n\n`
  }

  // funcsToSource takes a list of funtion nodes and returns an ifql script
  private funcsToSource = (funcs): string => {
    return funcs.reduce((acc, f, i) => {
      if (i === 0) {
        return `${f.source}`
      }

      return `${acc}\n\t${f.source}`
    }, '')
  }

  private getASTResponse = async (script: string) => {
    const {links} = this.props

    try {
      const ast = await getAST({url: links.ast, body: script})
      const body = bodyNodes(ast, this.state.suggestions)
      const status = {type: 'success', text: ''}
      this.setState({ast, script, body, status})
    } catch (error) {
      const s = error.data.slice(0, -5) // There is a null newline at the end of these responses
      const data = JSON.parse(s)
      const status = {type: 'error', text: `${data.message}`}
      this.setState({status})
      return console.error('Could not parse AST', error)
    }
  }

  private getTimeSeries = async () => {
    const {script} = this.state
    this.setState({data: 'fetching data...'})

    try {
      const {data} = await getTimeSeries(script)
      this.setState({data})
    } catch (error) {
      this.setState({data: 'Error fetching data'})
      console.error('Could not get timeSeries', error)
    }

    this.getASTResponse(script)
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
