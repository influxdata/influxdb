// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  OverlayContainer,
  OverlayHeading,
  OverlayBody,
  LabelSelector,
  ComponentSize,
  ComponentColor,
  Button,
  Grid,
  Form,
  ComponentStatus,
} from 'src/clockface'
import FetchLabels from 'src/shared/components/FetchLabels'

// Actions
import {
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
} from 'src/dashboards/actions/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard} from 'src/types/v2'
import {Label} from 'src/api'
import {RemoteDataState} from 'src/types'

interface Props {
  dashboard: Dashboard
  onDismissOverlay: () => void
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
}

interface State {
  selectedLabels: Label[]
  loading: RemoteDataState
}

@ErrorHandling
class EditDashboardLabelsOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      selectedLabels: props.dashboard.labels,
      loading: RemoteDataState.NotStarted,
    }
  }

  public render() {
    const {onDismissOverlay, dashboard} = this.props
    const {selectedLabels} = this.state

    return (
      <OverlayContainer maxWidth={720}>
        <OverlayHeading title="Manage Labels" onDismiss={onDismissOverlay} />
        <OverlayBody>
          <Form>
            <Grid>
              <Grid.Row>
                <Grid.Column>
                  <Form.Element label="">
                    <Form.Box>
                      <FetchLabels>
                        {labels => (
                          <LabelSelector
                            inputSize={ComponentSize.Medium}
                            allLabels={labels}
                            resourceType={dashboard.name}
                            selectedLabels={selectedLabels}
                            onAddLabel={this.handleAddLabel}
                            onRemoveLabel={this.handleRemoveLabel}
                            onRemoveAllLabels={this.handleRemoveAllLabels}
                          />
                        )}
                      </FetchLabels>
                    </Form.Box>
                  </Form.Element>
                  <Form.Footer>
                    <Button text="Cancel" onClick={onDismissOverlay} />
                    <Button
                      color={ComponentColor.Success}
                      text="Save Changes"
                      onClick={this.handleSaveLabels}
                      status={this.buttonStatus}
                    />
                  </Form.Footer>
                </Grid.Column>
              </Grid.Row>
            </Grid>
          </Form>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private get buttonStatus(): ComponentStatus {
    if (this.changes.isChanged) {
      if (this.state.loading === RemoteDataState.Loading) {
        return ComponentStatus.Loading
      }

      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
  }

  private get changes(): {
    isChanged: boolean
    removedLabels: Label[]
    addedLabels: Label[]
  } {
    const {dashboard} = this.props
    const {selectedLabels} = this.state

    const intersection = _.intersectionBy(
      dashboard.labels,
      selectedLabels,
      'name'
    )
    const removedLabels = _.differenceBy(dashboard.labels, intersection, 'name')
    const addedLabels = _.differenceBy(selectedLabels, intersection, 'name')

    return {
      isChanged: !!removedLabels.length || !!addedLabels.length,
      addedLabels,
      removedLabels,
    }
  }

  private handleRemoveAllLabels = (): void => {
    this.setState({selectedLabels: []})
  }

  private handleRemoveLabel = (label: Label): void => {
    const selectedLabels = this.state.selectedLabels.filter(
      l => l.name !== label.name
    )

    this.setState({selectedLabels})
  }

  private handleAddLabel = (label: Label): void => {
    const selectedLabels = [...this.state.selectedLabels, label]

    this.setState({selectedLabels})
  }

  private handleSaveLabels = async (): Promise<void> => {
    const {
      onAddDashboardLabels,
      onRemoveDashboardLabels,
      dashboard,
      onDismissOverlay,
    } = this.props

    const {addedLabels, removedLabels} = this.changes

    this.setState({loading: RemoteDataState.Loading})

    if (addedLabels.length) {
      await onAddDashboardLabels(dashboard.id, addedLabels)
    }

    if (removedLabels.length) {
      await onRemoveDashboardLabels(dashboard.id, removedLabels)
    }

    this.setState({loading: RemoteDataState.Done})

    onDismissOverlay()
  }
}

export default EditDashboardLabelsOverlay
