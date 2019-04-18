// Libraries
import React, {PureComponent} from 'react'

// Components
import {EmptyState} from '@influxdata/clockface'
import {IndexList, Overlay} from 'src/clockface'
import TokenRow from 'src/authorizations/components/TokenRow'
import ViewTokenOverlay from 'src/authorizations/components/ViewTokenOverlay'

// Types
import {Authorization} from '@influxdata/influx'
import {SortTypes} from 'src/shared/selectors/sort'
import {ComponentSize, Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/selectors/sort'

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
  sortedAuths: Authorization[]
}

export default class TokenList extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(props: Props) {
    return {
      sortedAuths: getSortedResources(props.auths, props),
    }
  }
  constructor(props) {
    super(props)
    this.state = {
      isTokenOverlayVisible: false,
      authInView: null,
      sortedAuths: this.props.auths,
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
            />
            <IndexList.HeaderCell columnName="Status" onClick={onClickColumn} />
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
    return ['description']
  }

  private get rows(): JSX.Element[] {
    const {sortedAuths} = this.state

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
        <EmptyState.Text text={emptyStateText} />
      </EmptyState>
    )
  }
}
