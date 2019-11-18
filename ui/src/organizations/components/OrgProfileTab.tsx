// Libraries
import React, {FunctionComponent} from 'react'
import {WithRouterProps, withRouter} from 'react-router'
import _ from 'lodash'

// Components
import {
  Form,
  Button,
  Alert,
  ComponentSize,
  Panel,
  IconFont,
  FlexBox,
  AlignItems,
  FlexDirection,
  InfluxColors,
  JustifyContent,
  Table,
  BorderType,
  ComponentColor,
  ButtonType,
} from '@influxdata/clockface'

// Types
interface OwnProps {
  orgID: string
  orgName: string
}

type Props = WithRouterProps & OwnProps

const OrgProfileTab: FunctionComponent<Props> = ({orgID, orgName, router}) => {
  const handleShowEditOverlay = () => {
    router.push(`/orgs/${orgID}/settings/profile/rename`)
  }

  return (
    <Panel backgroundColor={InfluxColors.Onyx}>
      <Panel.Header size={ComponentSize.Small}>
        <Panel.Title size={ComponentSize.Small}>
          About this Organization
        </Panel.Title>
      </Panel.Header>
      <Panel.Body size={ComponentSize.Small}>
        <Table
          borders={BorderType.All}
          style={{marginBottom: '18px', width: '100%'}}
        >
          <Table.Body>
            <Table.Row>
              <Table.Cell
                style={{width: '60px', borderColor: InfluxColors.Smoke}}
              >
                <strong>Name</strong>
              </Table.Cell>
              <Table.Cell style={{borderColor: InfluxColors.Smoke}}>
                {orgName}
              </Table.Cell>
            </Table.Row>
            <Table.Row>
              <Table.Cell
                style={{width: '60px', borderColor: InfluxColors.Smoke}}
              >
                <strong>ID</strong>
              </Table.Cell>
              <Table.Cell style={{borderColor: InfluxColors.Smoke}}>
                {orgID}
              </Table.Cell>
            </Table.Row>
          </Table.Body>
        </Table>
        <Form onSubmit={handleShowEditOverlay}>
          <Alert color={ComponentColor.Danger} icon={IconFont.AlertTriangle}>
            <FlexBox
              stretchToFitWidth={true}
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.SpaceBetween}
              style={{padding: '6px'}}
            >
              <div>
                <h4 style={{marginBottom: '0'}}>Danger Zone!</h4>
                <p style={{marginTop: '2px'}}>
                  This action can have wide-reaching unintended consequences.
                </p>
              </div>
              <Button
                color={ComponentColor.Danger}
                text="Rename Organization"
                icon={IconFont.Pencil}
                type={ButtonType.Submit}
              />
            </FlexBox>
          </Alert>
        </Form>
      </Panel.Body>
    </Panel>
  )
}

export default withRouter<OwnProps>(OrgProfileTab)
