// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {EmptyState, Overlay, IndexList} from '@influxdata/clockface'
import TokenRow from 'src/authorizations/components/TokenRow'
import ViewTokenOverlay from 'src/authorizations/components/ViewTokenOverlay'

// Types
import {Authorization} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {ComponentSize, Sort} from '@influxdata/clockface'

// Utils
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = keyof Authorization

interface Props {
  auths: Authorization[]
  searchTerm: string
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface State {
  isTokenOverlayVisible: boolean
  authInView: Authorization
}

export default class TokenList extends PureComponent<Props, State> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  constructor(props) {
    super(props)
    this.state = {
      isTokenOverlayVisible: false,
      authInView: null,
    }
  }

  public render() {
    const {sortKey, sortDirection, onClickColumn} = this.props
    const {isTokenOverlayVisible, authInView} = this.state

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              columnName="Description"
              onClick={onClickColumn}
              width="50%"
            />
            <IndexList.HeaderCell
              sortKey={this.headerKeys[1]}
              sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
              columnName="Status"
              onClick={onClickColumn}
              width="50%"
            />
          </IndexList.Header>
          <IndexList.Body emptyState={this.emptyState} columnCount={2}>
            {this.rows}
          </IndexList.Body>
        </IndexList>
        <Overlay visible={isTokenOverlayVisible}>
          <ViewTokenOverlay
            auth={authInView}
            onDismissOverlay={this.handleDismissOverlay}
          />
        </Overlay>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['description', 'status']
  }

  private get rows(): JSX.Element[] {
    const {auths, sortDirection, sortKey, sortType} = this.props
    const sortedAuths = this.memGetSortedResources(
      auths,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedAuths.map(auth => (
      <TokenRow
        key={auth.id}
        auth={auth}
        onClickDescription={this.handleClickDescription}
      />
    ))
  }

  private handleDismissOverlay = () => {
    this.setState({isTokenOverlayVisible: false})
  }

  private handleClickDescription = (authID: string): void => {
    const authInView = this.props.auths.find(a => a.id === authID)
    this.setState({isTokenOverlayVisible: true, authInView})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.props
    let emptyStateText =
      'There are not any Tokens associated with this account. Contact your administrator'

    if (searchTerm) {
      emptyStateText = 'No Tokens match your search term'
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>{emptyStateText}</EmptyState.Text>
      </EmptyState>
    )
  }
}
