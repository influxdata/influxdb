// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  ComponentSize,
  ComponentColor,
  ComponentStatus,
  Button,
} from '@influxdata/clockface'
import {
  OverlayContainer,
  OverlayHeading,
  OverlayBody,
  LabelSelector,
  Grid,
  Form,
} from 'src/clockface'
import FetchLabels from 'src/shared/components/FetchLabels'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Label} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

// Utils
import {getDeep} from 'src/utils/wrappers'

interface Props<T> {
  resource: T
  onDismissOverlay: () => void
  onAddLabels: (resourceID: string, labels: Label[]) => void
  onRemoveLabels: (resourceID: string, labels: Label[]) => void
}

interface State {
  selectedLabels: Label[]
  loading: RemoteDataState
}

@ErrorHandling
class EditLabelsOverlay<T> extends PureComponent<Props<T>, State> {
  constructor(props: Props<T>) {
    super(props)

    this.state = {
      selectedLabels: _.get(props, 'resource.labels'),
      loading: RemoteDataState.NotStarted,
    }
  }

  public render() {
    const {onDismissOverlay, resource} = this.props
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
                            resourceType={_.get(resource, 'name')}
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
    const {resource} = this.props
    const {selectedLabels} = this.state
    const labels = getDeep<Label[]>(resource, 'labels', [])

    const intersection = _.intersectionBy(labels, selectedLabels, 'name')
    const removedLabels = _.differenceBy(labels, intersection, 'name')
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
    const {onAddLabels, onRemoveLabels, resource, onDismissOverlay} = this.props

    const {addedLabels, removedLabels} = this.changes
    const resourceID = _.get(resource, 'id')

    this.setState({loading: RemoteDataState.Loading})

    if (addedLabels.length) {
      await onAddLabels(resourceID, addedLabels)
    }

    if (removedLabels.length) {
      await onRemoveLabels(resourceID, removedLabels)
    }

    this.setState({loading: RemoteDataState.Done})

    onDismissOverlay()
  }
}

export default EditLabelsOverlay
