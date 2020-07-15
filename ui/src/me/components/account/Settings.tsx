// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, Input, Button, Panel, Grid} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'
import {Columns, ComponentSize, ComponentStatus} from '@influxdata/clockface'

interface StateProps {
  me: AppState['me']
}

interface State {
  me: AppState['me']
}

export class Settings extends PureComponent<StateProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      me: this.props.me,
    }
  }

  public render() {
    const {me} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Six}>
            <Panel>
              <Panel.Header>
                <h4>About Me</h4>
                <Button text="Edit About Me" />
              </Panel.Header>
              <Panel.Body>
                <Form>
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
                </Form>
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleChangeInput = (_: ChangeEvent<HTMLInputElement>): void => {
    //  console.log('changing: ', e)
  }
}

const mstp = ({me}: AppState) => ({
  me,
})

export default connect(mstp)(Settings)
