// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  IndexList,
  EmptyState,
  ComponentSize,
  OverlayTechnology,
} from 'src/clockface'
import TokenRow from 'src/me/components/account/TokenRow'

// Types
import {Authorization} from '@influxdata/influx'
import ViewTokenOverlay from './ViewTokenOverlay'

// Actions
import {NotificationAction} from 'src/types'

interface Props {
  auths: Authorization[]
  onNotify: NotificationAction
  searchTerm: string
}

interface State {
  isTokenOverlayVisible: boolean
  authInView: Authorization
}

export default class TokenList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isTokenOverlayVisible: false,
      authInView: null,
    }
  }

  public render() {
    const {auths, onNotify} = this.props
    const {isTokenOverlayVisible, authInView} = this.state

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Description" />
            <IndexList.HeaderCell columnName="Status" />
          </IndexList.Header>
          <IndexList.Body emptyState={this.emptyState} columnCount={2}>
            {auths.map(a => {
              return (
                <TokenRow
                  key={a.id}
                  auth={a}
                  onClickDescription={this.handleClickDescription}
                />
              )
            })}
          </IndexList.Body>
        </IndexList>
        <OverlayTechnology visible={isTokenOverlayVisible}>
          <ViewTokenOverlay
            onNotify={onNotify}
            auth={authInView}
            onDismissOverlay={this.handleDismissOverlay}
          />
        </OverlayTechnology>
      </>
    )
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
