// Libraries
import React, {Component} from 'react'

// Components
import Body from 'src/clockface/components/index_views/IndexListBody'
import Header from 'src/clockface/components/index_views/IndexListHeader'
import HeaderCell from 'src/clockface/components/index_views/IndexListHeaderCell'
import Row from 'src/clockface/components/index_views/IndexListRow'
import Cell from 'src/clockface/components/index_views/IndexListRowCell'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Styles
import './IndexList.scss'

interface Props {
  children: JSX.Element[] | JSX.Element
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
