// Libraries
import React, {PureComponent} from 'react'

import _ from 'lodash'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  Alert,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
  ComponentSize,
  ButtonType,
  Form,
} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  message: string
  effectedItems: string[]
  onConfirm: () => void
  confirmButtonText: string
}

@ErrorHandling
class DangerConfirmationForm extends PureComponent<Props> {
  public render() {
    return (
      <Form onSubmit={this.props.onConfirm}>
        <ComponentSpacer
          alignItems={AlignItems.Center}
          direction={FlexDirection.Column}
          margin={ComponentSize.Large}
        >
          <Alert color={ComponentColor.Danger} icon={IconFont.AlertTriangle}>
            Bad things could happen if you don't read this!
          </Alert>
          <Form.Element label="">
            <>
              <p>{this.props.message}</p>
              <ul>
                {this.props.effectedItems.map(item => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </>
          </Form.Element>
          <Form.Footer>
            <Button
              color={ComponentColor.Danger}
              text={this.props.confirmButtonText}
              type={ButtonType.Submit}
            />
          </Form.Footer>
        </ComponentSpacer>
      </Form>
    )
  }
}

export default DangerConfirmationForm
