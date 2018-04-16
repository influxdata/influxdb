import React, {PureComponent} from 'react'

import {connect} from 'react-redux'
import uuid from 'uuid'
import _ from 'lodash'

import TimeMachine, {Suggestion} from 'src/ifql/components/TimeMachine'
import Walker from 'src/ifql/ast/walker'
import {Func} from 'src/ifql/components/FuncArgs'

import {getSuggestions, getAST} from 'src/ifql/apis'

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
  funcs: Func[]
  ast: object
  script: string
}

export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      suggestions: [],
      funcs: [],
      ast: null,
      script: 'from(db: "telegraf")\n\t|> filter() \n\t|> range(start: -15m)',
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
              funcs={this.state.funcs}
              suggestions={suggestions}
              onAddNode={this.handleAddNode}
              onSubmitScript={this.getASTResponse}
              onChangeScript={this.handleChangeScript}
              onDeleteFuncNode={this.handleDeleteFuncNode}
            />
          </div>
        </div>
      </div>
    )
  }

  private handleChangeScript = (script: string): void => {
    this.setState({script})
  }

  private handleAddNode = (name: string): void => {
    const script = `${this.state.script}\n\t|> ${name}()`
    this.getASTResponse(script)
  }

  private handleDeleteFuncNode = (id: string): void => {
    const funcs = this.state.funcs.filter(f => f.id !== id)
    const script = funcs.reduce((acc, f, i) => {
      if (i === 0) {
        return `${f.source}`
      }

      return `${acc}\n\t${f.source}`
    }, '')

    this.getASTResponse(script)
  }

  private funcs = (ast, suggestions): Func[] => {
    if (!ast) {
      return []
    }

    const walker = new Walker(ast)
    const functions = walker.functions.map(func => {
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
      const funcs = this.funcs(ast, this.state.suggestions)
      this.setState({ast, script, funcs})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
