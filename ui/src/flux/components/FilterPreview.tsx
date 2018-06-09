import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {getAST} from 'src/flux/apis'
import {Links, FilterNode} from 'src/types/flux'
import Walker from 'src/flux/ast/walker'
import FilterConditions from 'src/flux/components/FilterConditions'

interface Props {
  filterString?: string
  links: Links
}

interface State {
  nodes: FilterNode[]
  ast: object
}

export class FilterPreview extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    filterString: '',
  }

  constructor(props) {
    super(props)
    this.state = {
      nodes: [],
      ast: {},
    }
  }

  public async componentDidMount() {
    this.convertStringToNodes()
  }

  public async componentDidUpdate(prevProps, __) {
    if (this.props.filterString !== prevProps.filterString) {
      this.convertStringToNodes()
    }
  }

  public async convertStringToNodes() {
    const {links, filterString} = this.props

    const ast = await getAST({url: links.ast, body: filterString})
    const nodes = new Walker(ast).inOrderExpression
    this.setState({nodes, ast})
  }

  public render() {
    return <FilterConditions nodes={this.state.nodes} />
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.flux}
}

export default connect(mapStateToProps, null)(FilterPreview)
