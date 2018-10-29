// Libraries
import React, {Component} from 'react'

// Components
import Body from 'src/shared/components/index_views/IndexListBody'
import Header from 'src/shared/components/index_views/IndexListHeader'
import HeaderCell from 'src/shared/components/index_views/IndexListHeaderCell'
import Row from 'src/shared/components/index_views/IndexListRow'
import Cell from 'src/shared/components/index_views/IndexListRowCell'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
}

@ErrorHandling
class IndexList extends Component<Props> {
  public static Body = Body
  public static Header = Header
  public static HeaderCell = HeaderCell
  public static Row = Row
  public static Cell = Cell

  public render() {
    const {children} = this.props

    return <table className="index-list">{children}</table>
  }
}

export default IndexList
