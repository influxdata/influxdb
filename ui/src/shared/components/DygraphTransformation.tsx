import {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {FluxTable} from 'src/types'

import {
  fluxTablesToDygraph,
  FluxTablesToDygraphResult,
} from 'src/shared/parsing/flux/dygraph'

interface Props {
  tables: FluxTable[]
  children: (result: FluxTablesToDygraphResult) => JSX.Element
}

@ErrorHandling
class DygraphTransformation extends PureComponent<Props> {
  private fluxTablesToDygraph = memoizeOne(fluxTablesToDygraph)

  public render() {
    const {tables, children} = this.props
    const data = this.fluxTablesToDygraph(tables)

    return children(data)
  }
}

export default DygraphTransformation
