// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Button, ComponentColor} from '@influxdata/clockface'
import {
  OverlayTechnology,
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
  OverlayFooter,
} from 'src/clockface'

// Actions
import {
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/QueriesSwitcher.scss'

// Types
import {AppState, QueryEditMode, SourceType} from 'src/types/v2'

interface StateProps {
  editMode: QueryEditMode
  manuallyEdited: boolean
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
        <OverlayTechnology visible={isOverlayVisible}>
          <OverlayContainer maxWidth={400}>
            <OverlayHeading
              title="Are you sure?"
              onDismiss={this.handleDismissOverlay}
            />
            <OverlayBody>
              <p className="queries-switcher--warning">
                Switching to Query Builder mode will discard any changes you
                have made using Flux. This cannot be recovered.
              </p>
            </OverlayBody>
            <OverlayFooter>
              <Button text="Cancel" onClick={this.handleDismissOverlay} />
              <Button
                color={ComponentColor.Danger}
                text="Switch to Builder"
                onClick={this.handleConfirmSwitch}
              />
            </OverlayFooter>
          </OverlayContainer>
        </OverlayTechnology>
      </>
    )
  }

  private get button(): JSX.Element {
    const {editMode, onEditAsFlux} = this.props

    if (editMode !== QueryEditMode.Builder) {
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
    if (!this.props.manuallyEdited) {
      this.props.onEditWithBuilder()
      return
    }

    this.setState({isOverlayVisible: true})
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
  const {editMode, manuallyEdited} = getActiveQuery(state)
  return {editMode, sourceType: SourceType.V2, manuallyEdited}
}

const mdtp = {
  onEditWithBuilder: editActiveQueryWithBuilder,
  onEditAsFlux: editActiveQueryAsFlux,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueriesSwitcher)
