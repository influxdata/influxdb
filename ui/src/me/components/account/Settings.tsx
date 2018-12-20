// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Types
import {MeState} from 'src/types/v2'
import {
  Form,
  Button,
  Input,
  ComponentSize,
  ComponentStatus,
  Panel,
  Grid,
  Columns,
} from 'src/clockface'

interface StateProps {
  me: MeState
}

interface State {
  me: MeState
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
      <Panel>
        <Panel.Header title="About Me">
          <Button text="Edit About Me" />
        </Panel.Header>
        <Panel.Body>
          <Form>
            <Grid>
              <Grid.Row>
                <Grid.Column widthXS={Columns.Six}>
                  <Form.Element label="Username">
                    <Input
                      value={me.name}
                      dataTest="nameInput"
                      titleText="Username"
                      size={ComponentSize.Small}
                      status={ComponentStatus.Disabled}
                      onChange={this.handleChangeInput}
                    />
                  </Form.Element>
                </Grid.Column>
              </Grid.Row>
            </Grid>
          </Form>
        </Panel.Body>
      </Panel>
    )
  }

  private handleChangeInput = (_: ChangeEvent<HTMLInputElement>): void => {
    //  console.log('changing: ', e)
  }
}

const mstp = ({me}) => ({
  me,
})

export default connect<StateProps>(mstp)(Settings)
