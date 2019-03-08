// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Types
import {MeState} from 'src/types/v2'
import {
  Button,
  ComponentSize,
  ComponentStatus,
  Columns,
} from '@influxdata/clockface'
import {Form, Input, Panel, Grid, ColorPicker} from 'src/clockface'

interface StateProps {
  me: MeState
}

interface State {
  me: MeState
  color: string
}

export class Settings extends PureComponent<StateProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      me: this.props.me,
      color: '',
    }
  }

  public render() {
    const {me, color} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Six}>
            <Panel>
              <Panel.Header title="About Me">
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
            <ColorPicker
              selectedHex={color}
              onSelect={this.handleSelectColor}
            />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleSelectColor = (color: string): void => {
    this.setState({color})
  }

  private handleChangeInput = (_: ChangeEvent<HTMLInputElement>): void => {
    //  console.log('changing: ', e)
  }
}

const mstp = ({me}) => ({
  me,
})

export default connect<StateProps>(mstp)(Settings)
