import {PureComponent, ReactNode} from 'react'
import {connect} from 'react-redux'
import {getAST} from 'src/flux/apis'
import {Links, BinaryExpressionNode, MemberExpressionNode} from 'src/types/flux'
import Walker from 'src/flux/ast/walker'

interface Props {
  value: string
  links: Links
  render: (nodes: FilterNode[]) => ReactNode
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
    return this.props.render(this.state.nodes)
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.flux}
}

export default connect(mapStateToProps, null)(Filter)
