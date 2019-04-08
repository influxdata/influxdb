// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Form, Input, Button, ComponentColor} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'

// Types
import {AppState} from 'src/types'
import {ComponentSize, ComponentStatus} from '@influxdata/clockface'

interface StateProps {
  me: AppState['me']
}

type Props = StateProps & WithRouterProps

interface State {
  me: AppState['me']
}

export class Settings extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      me: this.props.me,
    }
  }

  public render() {
    const {me} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={500}>
          <Overlay.Heading
            title="Edit Profile"
            onDismiss={this.handleDismiss}
          />
          <Form>
            <Overlay.Body>
              <Form.Element label="Username">
                <Input
                  value={me.name}
                  testID="nameInput"
                  titleText="Username"
                  size={ComponentSize.Small}
                  status={ComponentStatus.Disabled}
                  onChange={this.handleChangeInput}
                />
              </Form.Element>
            </Overlay.Body>
            <Overlay.Footer>
              <Button text="Cancel" onClick={this.handleDismiss} />
              <Button
                text="Save"
                color={ComponentColor.Success}
                status={ComponentStatus.Disabled}
              />
            </Overlay.Footer>
          </Form>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleDismiss = (): void => {
    const {router} = this.props
    router.goBack()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>): void => {
    const me = {...this.state.me, name: e.target.value}
    this.setState({me})
  }
}

const mstp = ({me}: AppState) => ({
  me,
})

export default connect<StateProps>(mstp)(withRouter(Settings))
