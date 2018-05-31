import React, {PureComponent} from 'react'
import _ from 'lodash'

import TimeMachine from 'src/ifql/components/TimeMachine'
import IFQLHeader from 'src/ifql/components/IFQLHeader'
import {ErrorHandling} from 'src/shared/decorators/errors'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'

import {
  analyzeSuccess,
  ifqlTimeSeriesError,
} from 'src/shared/copy/notifications'
import {UpdateScript} from 'src/ifql/actions'

import {bodyNodes} from 'src/ifql/helpers'
import {getSuggestions, getAST, getTimeSeries} from 'src/ifql/apis'
import {funcNames, builder, argTypes} from 'src/ifql/constants'

import {Source, Service, Notification, FluxTable} from 'src/types'
import {
  Suggestion,
  FlatBody,
  Links,
  InputArg,
  Context,
  DeleteFuncNodeArgs,
  Func,
  ScriptStatus,
} from 'src/types/ifql'

interface Status {
  type: string
  text: string
}

interface Props {
  links: Links
  services: Service[]
  source: Source
  notify: (message: Notification) => void
  script: string
  updateScript: UpdateScript
}

interface Body extends FlatBody {
  id: string
}

interface State {
  body: Body[]
  ast: object
  data: FluxTable[]
  status: ScriptStatus
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
      data: [],
      suggestions: [],
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

    this.getTimeSeries()
  }

  public render() {
    const {suggestions, data, body, status} = this.state
    const {script} = this.props

    return (
      <IFQLContext.Provider value={this.getContext}>
        <KeyboardShortcuts onControlEnter={this.getTimeSeries}>
          <div className="page hosts-list-page">
            {this.header}
            <TimeMachine
              data={data}
              body={body}
              script={script}
              status={status}
              service={this.service}
              suggestions={suggestions}
              onAnalyze={this.handleAnalyze}
              onAppendFrom={this.handleAppendFrom}
              onAppendJoin={this.handleAppendJoin}
              onChangeScript={this.handleChangeScript}
              onSubmitScript={this.handleSubmitScript}
            />
          </div>
        </KeyboardShortcuts>
      </IFQLContext.Provider>
    )
  }

  private get header(): JSX.Element {
    const {services} = this.props

    if (!services.length) {
      return null
    }

    return (
      <IFQLHeader service={this.service} onGetTimeSeries={this.getTimeSeries} />
    )
  }

  private get service(): Service {
    return this.props.services[0]
  }

  private get getContext(): Context {
    return {
      onAddNode: this.handleAddNode,
      onChangeArg: this.handleChangeArg,
      onSubmitScript: this.handleSubmitScript,
      onChangeScript: this.handleChangeScript,
      onDeleteFuncNode: this.handleDeleteFuncNode,
      onGenerateScript: this.handleGenerateScript,
      service: this.service,
    }
  }

  private handleSubmitScript = () => {
    this.getASTResponse(this.props.script)
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

  private handleAppendFrom = (): void => {
    const {script} = this.props
    const newScript = `${script.trim()}\n\n${builder.NEW_FROM}\n\n`

    this.getASTResponse(newScript)
  }

  private handleAppendJoin = (): void => {
    const {script} = this.props
    const newScript = `${script.trim()}\n\n${builder.NEW_JOIN}\n\n`

    this.getASTResponse(newScript)
  }

  private handleChangeScript = (script: string): void => {
    this.props.updateScript(script)
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

  private handleAnalyze = async () => {
    const {links, notify, script} = this.props

    try {
      const ast = await getAST({url: links.ast, body: script})
      const body = bodyNodes(ast, this.state.suggestions)
      const status = {type: 'success', text: ''}
      notify(analyzeSuccess)

      this.setState({ast, body, status})
    } catch (error) {
      this.setState({status: this.parseError(error)})
      return console.error('Could not parse AST', error)
    }
  }

  private getASTResponse = async (script: string) => {
    const {links} = this.props

    if (!script) {
      return
    }

    try {
      const ast = await getAST({url: links.ast, body: script})
      const suggestions = this.state.suggestions.map(s => {
        if (s.name === funcNames.JOIN) {
          return {
            ...s,
            params: {
              tables: 'object',
              on: 'array',
              fn: 'function',
            },
          }
        }
        return s
      })
      const body = bodyNodes(ast, suggestions)
      const status = {type: 'success', text: ''}
      this.setState({ast, body, status})
      this.props.updateScript(script)
    } catch (error) {
      this.setState({status: this.parseError(error)})
      return console.error('Could not parse AST', error)
    }
  }

  private getTimeSeries = async () => {
    const {script, links, notify} = this.props

    if (!script) {
      return
    }

    try {
      await getAST({url: links.ast, body: script})
    } catch (error) {
      this.setState({status: this.parseError(error)})
      return console.error('Could not parse AST', error)
    }

    try {
      const data = await getTimeSeries(this.service, script)
      this.setState({data})
    } catch (error) {
      this.setState({data: []})

      notify(ifqlTimeSeriesError(error))
      console.error('Could not get timeSeries', error)
    }

    this.getASTResponse(script)
  }

  private parseError = (error): Status => {
    const s = error.data.slice(0, -5) // There is a 'null\n' at the end of these responses
    const data = JSON.parse(s)
    return {type: 'error', text: `${data.message}`}
  }
}

export default IFQLPage
