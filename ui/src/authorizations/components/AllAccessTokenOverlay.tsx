import React, {PureComponent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Alert,
  IconFont,
  ComponentColor,
  FlexBox,
  AlignItems,
  FlexDirection,
  ComponentSize,
  Button,
  ButtonType,
  Input,
  Overlay,
  Form,
} from '@influxdata/clockface'

// Actions
import {createAuthorization} from 'src/authorizations/actions/thunks'

// Utils
import {allAccessPermissions} from 'src/authorizations/utils/permissions'
import {getOrg} from 'src/organizations/selectors'
import {getMe} from 'src/me/selectors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState, Authorization} from 'src/types'

interface OwnProps {
  onClose: () => void
}

interface State {
  description: string
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

@ErrorHandling
class AllAccessTokenOverlay extends PureComponent<Props, State> {
  public state = {description: ''}

  render() {
    const {description} = this.state

    return (
      <Overlay.Container maxWidth={500}>
        <Overlay.Header
          title="Generate All Access Token"
          onDismiss={this.handleDismiss}
        />
        <Overlay.Body>
          <Form onSubmit={this.handleSave}>
            <FlexBox
              alignItems={AlignItems.Center}
              direction={FlexDirection.Column}
              margin={ComponentSize.Large}
            >
              <Alert
                icon={IconFont.AlertTriangle}
                color={ComponentColor.Warning}
              >
                This token will be able to create, update, delete, read, and
                write to anything in this organization
              </Alert>
              <Form.Element label="Description">
                <Input
                  placeholder="Describe this new token"
                  value={description}
                  onChange={this.handleInputChange}
                  testID="all-access-token-input"
                />
              </Form.Element>

              <Form.Footer>
                <Button
                  text="Cancel"
                  icon={IconFont.Remove}
                  onClick={this.handleDismiss}
                />

                <Button
                  text="Save"
                  testID="button--save"
                  icon={IconFont.Checkmark}
                  color={ComponentColor.Success}
                  type={ButtonType.Submit}
                />
              </Form.Footer>
            </FlexBox>
          </Form>
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private handleSave = () => {
    const {orgID, meID, onCreateAuthorization} = this.props

    const token: Authorization = {
      orgID,
      description: this.state.description,
      permissions: allAccessPermissions(orgID, meID),
    }

    onCreateAuthorization(token)

    this.handleDismiss()
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target

    this.setState({description: value})
  }

  private handleDismiss = () => {
    this.props.onClose()
  }
}

const mstp = (state: AppState) => {
  return {
    orgID: getOrg(state).id,
    meID: getMe(state).id,
  }
}

const mdtp = {
  onCreateAuthorization: createAuthorization,
}

const connector = connect(mstp, mdtp)

export default connector(AllAccessTokenOverlay)
