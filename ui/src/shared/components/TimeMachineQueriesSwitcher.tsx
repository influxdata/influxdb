// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Button,
  ComponentColor,
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
  editActiveQueryAsInfluxQL,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {
  getActiveQuery,
  getActiveQuerySource,
} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueriesSwitcher.scss'

// Types
import {AppState, QueryEditMode, Source} from 'src/types/v2'

interface StateProps {
  editMode: QueryEditMode
  sourceType: Source.TypeEnum
}

interface DispatchProps {
  onEditWithBuilder: typeof editActiveQueryWithBuilder
  onEditAsFlux: typeof editActiveQueryAsFlux
  onEditAsInfluxQL: typeof editActiveQueryAsInfluxQL
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
              <p className="time-machine-queries-switcher--warning">
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
    const {editMode, sourceType, onEditAsFlux, onEditAsInfluxQL} = this.props

    if (editMode !== QueryEditMode.Builder) {
      return (
        <Button
          text="Query Builder"
          titleText="Switch to Query Builder"
          onClick={this.handleShowOverlay}
        />
      )
    }

    if (sourceType === Source.TypeEnum.V1) {
      return (
        <Button
          text="Script Editor"
          titleText="Switch to Script Editor"
          onClick={onEditAsInfluxQL}
        />
      )
    }

    return (
      <Button
        text="Script Editor"
        titleText="Switch to Script Editor"
        onClick={onEditAsFlux}
      />
    )
  }

  private handleShowOverlay = (): void => {
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
  const editMode = getActiveQuery(state).editMode
  const sourceType = getActiveQuerySource(state).type

  return {editMode, sourceType}
}

const mdtp = {
  onEditWithBuilder: editActiveQueryWithBuilder,
  onEditAsFlux: editActiveQueryAsFlux,
  onEditAsInfluxQL: editActiveQueryAsInfluxQL,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueriesSwitcher)
