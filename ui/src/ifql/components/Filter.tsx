import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {getAST} from 'src/ifql/apis'
import {
  Links,
  OnChangeArg,
  BinaryExpressionNode,
  MemberExpressionNode,
} from 'src/types/ifql'
import Walker from 'src/ifql/ast/walker'

interface Props {
  argKey: string
  funcID: string
  bodyID: string
  declarationID: string
  value: string
  onChangeArg: OnChangeArg
  links: Links
}

type FilterNode = BinaryExpressionNode | MemberExpressionNode

interface State {
  nodes: FilterNode[]
}

export class Filter extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      nodes: [],
    }
  }

  public async componentDidMount() {
    const {links, value} = this.props
    try {
      const ast = await getAST({url: links.ast, body: value})
      const nodes = new Walker(ast).inOrderExpression
      this.setState({nodes})
    } catch (error) {
      console.error('Could not parse AST', error)
    }
  }

  public render() {
    const {argKey} = this.props
    const {nodes} = this.state

    return (
      <div className="func-arg">
        <label className="func-arg--label">{argKey}</label>
        {nodes.map((n, i) => {
          return <div key={i}>{n.source}</div>
        })}
      </div>
    )
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(Filter)
