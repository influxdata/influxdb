import React, {PureComponent} from 'react'

import {connect} from 'react-redux'
import _ from 'lodash'

import TimeMachine, {Suggestion} from 'src/ifql/components/TimeMachine'
import Walker from 'src/ifql/ast/walker'

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
  ast: object
  query: string
}

export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      suggestions: [],
      ast: null,
      query: 'from(db: "telegraf") |> filter() |> range(start: -15m)',
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

    this.getASTResponse(this.state.query)
  }

  public render() {
    const {suggestions} = this.state

    return (
      <div className="page">
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
              suggestions={suggestions}
              funcs={this.funcs}
              onAddNode={this.handleAddNode}
            />
          </div>
        </div>
      </div>
    )
  }

  private handleAddNode = (name: string) => {
    const query = `${this.state.query} |> ${name}()`
    this.getASTResponse(query)
  }

  private get funcs() {
    const {ast, suggestions} = this.state

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
        name,
        args,
      }
    })

    return functions
  }

  private async getASTResponse(query: string) {
    const {links} = this.props

    try {
      const ast = await getAST({url: links.ast, body: query})
      this.setState({ast, query})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
