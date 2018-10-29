import {PureComponent} from 'react'
import {FluxTable} from 'src/types'

import {
  fluxTablesToDygraph,
  FluxTablesToDygraphResult,
} from 'src/shared/parsing/flux/dygraph'

interface Props {
  tables: FluxTable[]
  children: (result: FluxTablesToDygraphResult) => JSX.Element
}

class DygraphTransformation extends PureComponent<
  Props,
  FluxTablesToDygraphResult
> {
  public static getDerivedStateFromProps(props) {
    return {...fluxTablesToDygraph(props.tables)}
  }

  constructor(props) {
    super(props)

    this.state = {
      labels: [],
      dygraphsData: [],
      nonNumericColumns: [],
    }
  }

  public render() {
    const {children} = this.props
    return children(this.state)
  }
}

export default DygraphTransformation
