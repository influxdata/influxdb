import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {getAST} from 'src/flux/apis'
import {tagKeys as fetchTagKeys} from 'src/shared/apis/flux/metaQueries'
import parseValuesColumn from 'src/shared/parsing/flux/values'
import FilterTagList from 'src/flux/components/FilterTagList'
import Walker from 'src/flux/ast/walker'

import {Service} from 'src/types'
import {Links, OnChangeArg, Func, FilterNode} from 'src/types/flux'

interface Props {
  links: Links
  value: string
  func: Func
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  db: string
  service: Service
  onGenerateScript: () => void
}

interface State {
  tagKeys: string[]
  nodes: FilterNode[]
  ast: object
}

class FilterArgs extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      tagKeys: [],
      nodes: [],
      ast: {},
    }
  }

  public async convertStringToNodes() {
    const {links, value} = this.props

    const ast = await getAST({url: links.ast, body: value})
    const nodes = new Walker(ast).inOrderExpression
    this.setState({nodes, ast})
  }

  public componentDidUpdate(prevProps) {
    if (prevProps.value !== this.props.value) {
      this.convertStringToNodes()
    }
  }

  public async componentDidMount() {
    const {db, service} = this.props

    try {
      this.convertStringToNodes()
      const response = await fetchTagKeys(service, db, [])
      const tagKeys = parseValuesColumn(response)
      this.setState({tagKeys})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {
      db,
      service,
      onChangeArg,
      func,
      bodyID,
      declarationID,
      onGenerateScript,
    } = this.props
    const {nodes} = this.state

    return (
      <FilterTagList
        db={db}
        service={service}
        tags={this.state.tagKeys}
        filter={[]}
        onChangeArg={onChangeArg}
        func={func}
        nodes={nodes}
        bodyID={bodyID}
        declarationID={declarationID}
        onGenerateScript={onGenerateScript}
      />
    )
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.flux}
}

export default connect(mapStateToProps, null)(FilterArgs)
