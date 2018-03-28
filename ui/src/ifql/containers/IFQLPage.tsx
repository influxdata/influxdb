import React, {PureComponent} from 'react'

import {connect} from 'react-redux'

import TimeMachine from 'src/ifql/components/TimeMachine'
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
  funcs: string[]
  ast: object
}

export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      funcs: [],
      ast: null,
    }
  }

  public async componentDidMount() {
    const {links} = this.props
    const {suggestions} = links
    const baseQuery = 'from()'

    try {
      const results = await getSuggestions(suggestions)
      const funcs = results.map(s => s.name)
      this.setState({funcs})
    } catch (error) {
      console.error('Could not get function suggestions: ', error)
    }

    try {
      const ast = await getAST({url: links.ast, body: baseQuery})
      this.setState({ast})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }

  public render() {
    const {funcs} = this.state

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
              funcs={funcs}
              nodes={this.nodes}
              onAddNode={this.handleAddNode}
            />
          </div>
        </div>
      </div>
    )
  }

  private handleAddNode = (name: string) => {
    console.log(name)
    // Do a flip
  }

  private get nodes() {
    const {ast} = this.state

    if (!ast) {
      return []
    }

    const walker = new Walker(ast)

    return walker.functions
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
