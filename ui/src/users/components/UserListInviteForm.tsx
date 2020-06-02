// Libraries
import React, {ChangeEvent, FC, useContext, useState} from 'react'
import {get} from 'lodash'

// Components
import {UserListContext, UserListContextResult} from './UserListContainer'
import UserRoleDropdown from './UserRoleDropdown'
import UserInviteSubmit from './UserInviteSubmit'
import {
  Columns,
  ComponentSize,
  FontWeight,
  Form,
  Gradients,
  Grid,
  Heading,
  HeadingElement,
  IconFont,
  Input,
  Notification,
  Panel,
} from '@influxdata/clockface'

import {postOrgsInvite} from 'src/client/unifyRoutes'

// Actions
import {editDraftInvite, resetDraftInvite, setInvites} from '../reducers'

// Hooks
import {useNotify} from 'src/users/hooks/useNotify'

// Types
import {RemoteDataState, Invite} from 'src/types'

interface InviteErrors {
  email?: string
}

const UserListInviteForm: FC = () => {
  const [errors, setErrors] = useState<InviteErrors>({})
  const [notify, {show, hide}] = useNotify()
  const [{draftInvite, orgID, invites}, dispatch] = useContext<
    UserListContextResult
  >(UserListContext)

  const onInviteUser = async () => {
    dispatch(resetDraftInvite())

    try {
      const resp = await postOrgsInvite({orgID, data: draftInvite as Invite})

      if (resp.status !== 201) {
        throw Error(resp.data.message)
      }

      const invite = {...resp.data, status: RemoteDataState.Done}

      dispatch(setInvites([invite, ...invites]))
      show()
    } catch (error) {
      console.error(error)

      if (!error.response) {
        return
      }

      const {status, data} = error.response

      if (status === 422) {
        const {errors} = data

        setErrors({
          email: get(errors, ['email', '0'], ''),
        })
      }
    }
  }

  const onChangeInvitee = (e: ChangeEvent<HTMLInputElement>) => {
    setErrors({})
    dispatch(editDraftInvite({...draftInvite, email: e.target.value}))
  }

  return (
    <>
      <Grid>
        <Grid.Row>
          <Grid.Column widthMD={Columns.Ten} widthLG={Columns.Six}>
            <Panel
              gradient={Gradients.PolarExpress}
              className="user-list-invite--form-panel"
            >
              <Panel.Header>
                <Heading
                  weight={FontWeight.Light}
                  element={HeadingElement.H2}
                  className="user-list-invite--form-heading"
                >
                  Add a new user to your organization
                </Heading>
              </Panel.Header>
              <Panel.Body size={ComponentSize.Small}>
                <Form
                  onSubmit={onInviteUser}
                  className="user-list-invite--form"
                >
                  <Form.Element
                    label="Enter user Email Address"
                    className="element email--input"
                    errorMessage={errors.email}
                  >
                    <Input
                      placeholder="email address"
                      onChange={onChangeInvitee}
                      value={draftInvite.email}
                      required={true}
                    />
                  </Form.Element>
                  <Form.Element
                    label="Assign a Role"
                    className="element role--dropdown"
                  >
                    <UserRoleDropdown />
                  </Form.Element>
                  <Form.Element label="" className="element submit--button">
                    <UserInviteSubmit draftInvite={draftInvite} />
                  </Form.Element>
                </Form>
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <Notification
        size={ComponentSize.Small}
        gradient={Gradients.HotelBreakfast}
        icon={IconFont.UserAdd}
        onDismiss={hide}
        onTimeout={hide}
        visible={notify}
        duration={5000}
      >
        Invitation Sent
      </Notification>
    </>
  )
}

export default UserListInviteForm
