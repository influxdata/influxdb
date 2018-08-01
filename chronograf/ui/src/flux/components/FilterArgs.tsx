// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FilterTagList from 'src/flux/components/FilterTagList'

// APIs
import {getAST} from 'src/flux/apis'
import {tagKeys as fetchTagKeys} from 'src/shared/apis/flux/metaQueries'

// Utils
import parseValuesColumn from 'src/shared/parsing/flux/values'
import Walker from 'src/flux/ast/walker'
import {makeCancelable} from 'src/utils/promises'

// Types
import {Source} from 'src/types/v2'
import {Links, OnChangeArg, Func, FilterNode} from 'src/types/flux'
import {WrappedCancelablePromise} from 'src/types/promises'

interface Props {
  links: Links
  value: string
  func: Func
  bodyID: string
  declarationID: string
  onChangeArg: OnChangeArg
  db: string
  source: Source
  onGenerateScript: () => void
}

interface State {
  tagKeys: string[]
  nodes: FilterNode[]
  ast: object
}

class FilterArgs extends PureComponent<Props, State> {
  private fetchTagKeysResponse?: WrappedCancelablePromise<string>

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
    try {
      this.convertStringToNodes()
      const response = await this.getTagKeys()
      const tagKeys = parseValuesColumn(response)

      this.setState({
        tagKeys,
      })
    } catch (error) {
      if (!error.isCanceled) {
        console.error(error)
      }
    }
  }

  public componentWillUnmount() {
    if (this.fetchTagKeysResponse) {
      this.fetchTagKeysResponse.cancel()
    }
  }

  public render() {
    const {
      db,
      source,
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
        source={source}
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

  private getTagKeys(): Promise<string> {
    const {db, source} = this.props

    this.fetchTagKeysResponse = makeCancelable(fetchTagKeys(source, db, []))

    return this.fetchTagKeysResponse.promise
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.flux}
}

export default connect(mapStateToProps, null)(FilterArgs)
