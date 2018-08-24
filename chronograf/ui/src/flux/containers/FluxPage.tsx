// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

/// Components
import TimeMachine from 'src/flux/components/TimeMachine'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

// APIs
import {getSuggestions, getAST, getTimeSeries} from 'src/flux/apis'

// Constants
import {
  validateSuccess,
  fluxTimeSeriesError,
  fluxResponseTruncatedError,
} from 'src/shared/copy/notifications'
import {builder, argTypes, emptyAST} from 'src/flux/constants'

// Actions
import {
  UpdateScript,
  updateScript as updateScriptAction,
} from 'src/flux/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Utils
import {bodyNodes} from 'src/flux/helpers'

// Types
import {Source} from 'src/types/v2'
import {Notification, FluxTable} from 'src/types'
import {
  Suggestion,
  FlatBody,
  Links,
  InputArg,
  Context,
  DeleteFuncNodeArgs,
  Func,
  ScriptStatus,
} from 'src/types/flux'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Status {
  type: string
  text: string
}

interface Props {
  links: Links
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

type ScriptFunc = (script: string) => void

export const FluxContext = React.createContext()

@ErrorHandling
export class FluxPage extends PureComponent<Props, State> {
  private debouncedASTResponse: ScriptFunc

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

    this.debouncedASTResponse = _.debounce(script => {
      this.getASTResponse(script, false)
    }, 250)
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
    const {suggestions, body, status} = this.state
    const {script, source} = this.props

    return (
      <FluxContext.Provider value={this.getContext}>
        <KeyboardShortcuts onControlEnter={this.getTimeSeries}>
          <div className="page hosts-list-page">
            <PageHeader titleText="Flux Editor" fullWidth={true} />
            <TimeMachine
              body={body}
              script={script}
              status={status}
              source={source}
              suggestions={suggestions}
              onValidate={this.handleValidate}
              onAppendFrom={this.handleAppendFrom}
              onAppendJoin={this.handleAppendJoin}
              onChangeScript={this.handleChangeScript}
              onSubmitScript={this.handleSubmitScript}
              onDeleteBody={this.handleDeleteBody}
            />
          </div>
        </KeyboardShortcuts>
      </FluxContext.Provider>
    )
  }

  private get getContext(): Context {
    return {
      onAddNode: this.handleAddNode,
      onChangeArg: this.handleChangeArg,
      onSubmitScript: this.handleSubmitScript,
      onChangeScript: this.handleChangeScript,
      onDeleteFuncNode: this.handleDeleteFuncNode,
      onGenerateScript: this.handleGenerateScript,
      onToggleYield: this.handleToggleYield,
      data: this.state.data,
      scriptUpToYield: this.handleScriptUpToYield,
      source: this.props.source,
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
    return this.getBodyToScript(this.state.body)
  }

  private getBodyToScript(body: Body[]): string {
    return body.reduce((acc, b) => {
      if (b.declarations.length) {
        const declaration = _.get(b, 'declarations.0', false)
        if (!declaration) {
          return acc
        }

        if (!declaration.funcs) {
          return `${acc}${b.source}`
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
          return `${key}: ["${value}"]`
        }

        if (type === argTypes.OBJECT) {
          const valueString = _.map(value, (v, k) => k + ':' + v).join(',')
          return `${key}: {${valueString}}`
        }

        return `${key}: ${value}`
      })
      .join(', ')
  }

  private handleAppendFrom = (): void => {
    const {script} = this.props
    let newScript = script.trim()
    const from = builder.NEW_FROM

    if (!newScript) {
      this.getASTResponse(from)
      return
    }

    newScript = `${script.trim()}\n\n${from}\n\n`
    this.getASTResponse(newScript)
  }

  private handleAppendJoin = (): void => {
    const {script} = this.props
    const newScript = `${script.trim()}\n\n${builder.NEW_JOIN}\n\n`

    this.getASTResponse(newScript)
  }

  private handleChangeScript = (script: string): void => {
    this.debouncedASTResponse(script)
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

  private handleDeleteBody = (bodyID: string): void => {
    const newBody = this.state.body.filter(b => b.id !== bodyID)
    const script = this.getBodyToScript(newBody)

    this.getASTResponse(script)
  }

  private handleScriptUpToYield = (
    bodyID: string,
    declarationID: string,
    funcNodeIndex: number,
    isYieldable: boolean
  ): string => {
    const {body: bodies} = this.state

    const bodyIndex = bodies.findIndex(b => b.id === bodyID)

    const bodiesBeforeYield = bodies
      .slice(0, bodyIndex)
      .map(b => this.removeYieldFuncFromBody(b))

    const body = this.prepBodyForYield(
      bodies[bodyIndex],
      declarationID,
      funcNodeIndex
    )

    const bodiesForScript = [...bodiesBeforeYield, body]

    let script = this.getBodyToScript(bodiesForScript)

    if (!isYieldable) {
      const regex: RegExp = /\n{2}$/
      script = script.replace(regex, '\n\t|> last()\n\t|> yield()$&')
      return script
    }

    return script
  }

  private prepBodyForYield(
    body: Body,
    declarationID: string,
    yieldNodeIndex: number
  ) {
    const funcs = this.getFuncs(body, declarationID)
    const funcsUpToYield = funcs.slice(0, yieldNodeIndex)
    const yieldNode = funcs[yieldNodeIndex]
    const funcsWithoutYields = funcsUpToYield.filter(f => f.name !== 'yield')
    const funcsForBody = [...funcsWithoutYields, yieldNode]

    if (declarationID) {
      const declaration = body.declarations.find(d => d.id === declarationID)
      const declarations = [{...declaration, funcs: funcsForBody}]
      return {...body, declarations}
    }

    return {...body, funcs: funcsForBody}
  }

  private getFuncs(body: Body, declarationID: string): Func[] {
    const declaration = body.declarations.find(d => d.id === declarationID)

    if (declaration) {
      return _.get(declaration, 'funcs', [])
    }
    return _.get(body, 'funcs', [])
  }

  private removeYieldFuncFromBody(body: Body): Body {
    const declarationID = _.get(body, 'declarations.0.id')
    const funcs = this.getFuncs(body, declarationID)

    if (_.isEmpty(funcs)) {
      return body
    }

    const funcsWithoutYields = funcs.filter(f => f.name !== 'yield')

    if (declarationID) {
      const declaration = _.get(body, 'declarations.0')
      const declarations = [{...declaration, funcs: funcsWithoutYields}]
      return {...body, declarations}
    }

    return {...body, funcs: funcsWithoutYields}
  }

  private handleToggleYield = (
    bodyID: string,
    declarationID: string,
    funcNodeIndex: number
  ): void => {
    const script = this.state.body.reduce((acc, body) => {
      const {id, source, funcs} = body

      if (id === bodyID) {
        const declaration = body.declarations.find(d => d.id === declarationID)

        if (declaration) {
          return `${acc}${declaration.name} = ${this.addOrRemoveYieldFunc(
            declaration.funcs,
            funcNodeIndex
          )}`
        }

        return `${acc}${this.addOrRemoveYieldFunc(funcs, funcNodeIndex)}`
      }

      return `${acc}${this.formatSource(source)}`
    }, '')

    this.getASTResponse(script)
  }

  private getNextYieldName = (): string => {
    const yieldNamePrefix = 'results_'
    const yieldNamePattern = `${yieldNamePrefix}(\\d+)`
    const regex = new RegExp(yieldNamePattern)

    const MIN = -1

    const yieldsMaxResultNumber = this.state.body.reduce((scriptMax, body) => {
      const {funcs: bodyFuncs, declarations} = body

      let funcs = bodyFuncs

      if (!_.isEmpty(declarations)) {
        funcs = _.flatMap(declarations, d => _.get(d, 'funcs', []))
      }

      const yields = funcs.filter(f => f.name === 'yield')
      const bodyMax = yields.reduce((max, y) => {
        const yieldArg = _.get(y, 'args.0.value')

        if (!yieldArg) {
          return max
        }

        const yieldNumberString = _.get(yieldArg.match(regex), '1', `${MIN}`)
        const yieldNumber = parseInt(yieldNumberString, 10)

        return Math.max(yieldNumber, max)
      }, scriptMax)

      return Math.max(scriptMax, bodyMax)
    }, MIN)

    return `${yieldNamePrefix}${yieldsMaxResultNumber + 1}`
  }

  private addOrRemoveYieldFunc(funcs: Func[], funcNodeIndex: number): string {
    if (funcNodeIndex < funcs.length - 1) {
      const funcAfterNode = funcs[funcNodeIndex + 1]

      if (funcAfterNode.name === 'yield') {
        return this.removeYieldFunc(funcs, funcAfterNode)
      }
    }

    return this.insertYieldFunc(funcs, funcNodeIndex)
  }

  private removeYieldFunc(funcs: Func[], funcAfterNode: Func): string {
    const filteredFuncs = funcs.filter(f => f.id !== funcAfterNode.id)

    return `${this.funcsToScript(filteredFuncs)}\n\n`
  }

  private appendFunc = (funcs: Func[], name: string): string => {
    return `${this.funcsToScript(funcs)}\n\t|> ${name}()\n\n`
  }

  private insertYieldFunc(funcs: Func[], index: number): string {
    const funcsBefore = funcs.slice(0, index + 1)
    const funcsBeforeScript = this.funcsToScript(funcsBefore)

    const funcsAfter = funcs.slice(index + 1)
    const funcsAfterScript = this.funcsToScript(funcsAfter)

    const funcSeparator = '\n\t|> '

    if (funcsAfterScript) {
      return `${funcsBeforeScript}${funcSeparator}yield(name: "${this.getNextYieldName()}")${funcSeparator}${funcsAfterScript}\n\n`
    }

    return `${funcsBeforeScript}${funcSeparator}yield(name: "${this.getNextYieldName()}")\n\n`
  }

  private handleDeleteFuncNode = (ids: DeleteFuncNodeArgs): void => {
    const {funcID, declarationID = '', bodyID, yieldNodeID = ''} = ids

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

          const functions = declaration.funcs.filter(
            f => f.id !== funcID && f.id !== yieldNodeID
          )

          const s = this.funcsToSource(functions)
          return `${declaration.name} = ${this.formatLastSource(s, isLast)}`
        }

        const funcs = body.funcs.filter(
          f => f.id !== funcID && f.id !== yieldNodeID
        )
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

  // funcsToSource takes a list of funtion nodes and returns an flux script
  private funcsToSource = (funcs): string => {
    return funcs.reduce((acc, f, i) => {
      if (i === 0) {
        return `${f.source}`
      }

      return `${acc}\n\t${f.source}`
    }, '')
  }

  private handleValidate = async () => {
    const {links, notify, script} = this.props

    try {
      const ast = await getAST({url: links.ast, body: script})
      const body = bodyNodes(ast, this.state.suggestions)
      const status = {type: 'success', text: ''}
      notify(validateSuccess())

      this.setState({ast, body, status})
    } catch (error) {
      this.setState({status: this.parseError(error)})
      return console.error('Could not parse AST', error)
    }
  }

  private getASTResponse = async (script: string, update: boolean = true) => {
    const {links} = this.props

    if (!script) {
      this.props.updateScript(script)
      return this.setState({ast: emptyAST, body: []})
    }

    try {
      const ast = await getAST({url: links.ast, body: script})

      if (update) {
        this.props.updateScript(script)
      }

      const body = bodyNodes(ast, this.state.suggestions)
      const status = {type: 'success', text: ''}
      this.setState({ast, body, status})
    } catch (error) {
      this.setState({status: this.parseError(error)})
      return console.error('Could not parse AST', error)
    }
  }

  private getTimeSeries = async () => {
    const {script, links, notify, source} = this.props

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
      const {tables, didTruncate} = await getTimeSeries(
        source.links.query,
        script
      )

      this.setState({data: tables})

      if (didTruncate) {
        notify(fluxResponseTruncatedError())
      }
    } catch (error) {
      this.setState({data: []})

      notify(fluxTimeSeriesError(error))
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

const mdtp = {
  updateScript: updateScriptAction,
  notify: notifyAction,
}

const mstp = ({links, script}) => {
  return {
    links: links.flux,
    script,
  }
}

export default connect(mstp, mdtp)(FluxPage)
