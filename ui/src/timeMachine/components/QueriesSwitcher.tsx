// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor, Overlay} from '@influxdata/clockface'

// Actions
import {
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'
import {hasQueryBeenEdited} from 'src/timeMachine/utils/queryBuilder'

// Types
import {AppState, DashboardQuery} from 'src/types'

interface StateProps {
  activeQuery: DashboardQuery
}

interface DispatchProps {
  onEditWithBuilder: typeof editActiveQueryWithBuilder
  onEditAsFlux: typeof editActiveQueryAsFlux
}

type Props = StateProps & DispatchProps

interface State {
  isOverlayVisible: boolean
}

class TimeMachineQueriesSwitcher extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isOverlayVisible: false,
    }
  }

  public render() {
    const {isOverlayVisible} = this.state

    return (
      <>
        {this.button}
        <Overlay visible={isOverlayVisible}>
          <Overlay.Container maxWidth={400}>
            <Overlay.Header
              title="Are you sure?"
              onDismiss={this.handleDismissOverlay}
            />
            <Overlay.Body>
              <p className="queries-switcher--warning">
                Switching to Query Builder mode will discard any changes you
                have made using Flux. This cannot be recovered.
              </p>
            </Overlay.Body>
            <Overlay.Footer>
              <Button text="Cancel" onClick={this.handleDismissOverlay} />
              <Button
                color={ComponentColor.Danger}
                text="Switch to Builder"
                onClick={this.handleConfirmSwitch}
              />
            </Overlay.Footer>
          </Overlay.Container>
        </Overlay>
      </>
    )
  }

  private get button(): JSX.Element {
    const {onEditAsFlux} = this.props
    const {editMode} = this.props.activeQuery

    if (editMode !== 'builder') {
      return (
        <Button
          text="Query Builder"
          titleText="Switch to Query Builder"
          onClick={this.handleShowOverlay}
          testID="switch-to-query-builder"
        />
      )
    }

    return (
      <Button
        text="Script Editor"
        titleText="Switch to Script Editor"
        onClick={onEditAsFlux}
        testID="switch-to-script-editor"
      />
    )
  }

  private handleShowOverlay = (): void => {
    const {text, builderConfig} = this.props.activeQuery

    if (hasQueryBeenEdited(text, builderConfig)) {
      // If a user will lose changes by switching to builder mode, show a modal
      // that asks them to confirm the mode switch
      this.setState({isOverlayVisible: true})
    } else {
      this.props.onEditWithBuilder()
    }
  }

  private handleDismissOverlay = (): void => {
    this.setState({isOverlayVisible: false})
  }

  private handleConfirmSwitch = (): void => {
    const {onEditWithBuilder} = this.props

    this.handleDismissOverlay()
    onEditWithBuilder()
  }
}

const mstp = (state: AppState) => {
  const activeQuery = getActiveQuery(state)

  return {activeQuery}
}

const mdtp = {
  onEditWithBuilder: editActiveQueryWithBuilder,
  onEditAsFlux: editActiveQueryAsFlux,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueriesSwitcher)
