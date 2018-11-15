// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
  Button,
  ComponentColor,
  ComponentStatus,
  Form,
  Input,
  Radio,
} from 'src/clockface'

// Actions
import {createSource} from 'src/sources/actions'
import {notify} from 'src/shared/actions/notifications'

// Utils
import {sourceCreationFailed} from 'src/shared/copy/notifications'

// Styles
import 'src/sources/components/CreateSourceOverlay.scss'

// Types
import {Source} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

interface DispatchProps {
  onCreateSource: typeof createSource
  onNotify: typeof notify
}

interface OwnProps {
  onHide: () => void
}

type Props = DispatchProps & OwnProps

interface State {
  draftSource: Partial<Source>
  creationStatus: RemoteDataState
}

class CreateSourceOverlay extends PureComponent<Props, State> {
  public state: State = {
    draftSource: {
      name: '',
      type: 'v1',
      url: '',
    },
    creationStatus: RemoteDataState.NotStarted,
  }

  public render() {
    const {onHide} = this.props
    const {draftSource} = this.state

    return (
      <div className="create-source-overlay">
        <OverlayContainer>
          <OverlayHeading title="Create Source">
            <div className="create-source-overlay--heading-buttons">
              <Button text="Cancel" onClick={onHide} />
              <Button
                text="Save"
                color={ComponentColor.Success}
                status={this.saveButtonStatus}
                onClick={this.handleSave}
              />
            </div>
          </OverlayHeading>
          <OverlayBody>
            <Form>
              <Form.Element label="Name">
                <Input
                  name="name"
                  autoFocus={true}
                  value={draftSource.name}
                  onChange={this.handleInputChange}
                />
              </Form.Element>
              <Form.Element label="URL">
                <Input
                  name="url"
                  autoFocus={true}
                  value={draftSource.url}
                  onChange={this.handleInputChange}
                />
              </Form.Element>
              <Form.Element label="Type">
                <Radio>
                  <Radio.Button
                    active={draftSource.type === 'v1'}
                    onClick={this.handleChangeType}
                    value="v1"
                  >
                    v1
                  </Radio.Button>
                  <Radio.Button
                    active={draftSource.type === 'v2'}
                    onClick={this.handleChangeType}
                    value="v2"
                  >
                    v2
                  </Radio.Button>
                </Radio>
              </Form.Element>
            </Form>
          </OverlayBody>
        </OverlayContainer>
      </div>
    )
  }

  private get saveButtonStatus(): ComponentStatus {
    return ComponentStatus.Default
  }

  private handleSave = async () => {
    const {onCreateSource, onNotify, onHide} = this.props
    const {draftSource} = this.state

    this.setState({creationStatus: RemoteDataState.Loading})

    try {
      await onCreateSource(draftSource)
      onHide()
    } catch (error) {
      this.setState({creationStatus: RemoteDataState.Error})
      onNotify(sourceCreationFailed(error.toString()))
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const draftSource = {
      ...this.state.draftSource,
      [e.target.name]: e.target.value,
    }

    this.setState({draftSource})
  }

  private handleChangeType = (type: 'v1' | 'v2') => {
    const draftSource = {
      ...this.state.draftSource,
      type,
    }

    this.setState({draftSource})
  }
}

const mdtp = {
  onCreateSource: createSource,
  onNotify: notify,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(CreateSourceOverlay)
