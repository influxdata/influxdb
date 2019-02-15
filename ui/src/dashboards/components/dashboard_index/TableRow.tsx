// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {
  Stack,
  Button,
  IconFont,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'
import {
  IndexList,
  ConfirmationButton,
  Label,
  ComponentSpacer,
} from 'src/clockface'
import EditableDescription from 'src/shared/components/editable_description/EditableDescription'

// Types
import {Dashboard, Organization} from 'src/types/v2'
import {Alignment} from 'src/clockface'
import moment from 'moment'

// Constants
import {
  UPDATED_AT_TIME_FORMAT,
  DEFAULT_DASHBOARD_NAME,
} from 'src/dashboards/constants'
import EditableName from 'src/shared/components/EditableName'

interface Props {
  dashboard: Dashboard
  orgs: Organization[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onEditLabels: (dashboard: Dashboard) => void
  showOwnerColumn: boolean
}

export default class DashboardsIndexTableRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onDeleteDashboard} = this.props
    const {id} = dashboard

    return (
      <IndexList.Row key={`dashboard-id--${id}`} disabled={false}>
        <IndexList.Cell>
          <ComponentSpacer
            stackChildren={Stack.Rows}
            align={Alignment.Left}
            stretchToFitWidth={true}
          >
            <ComponentSpacer
              stackChildren={Stack.Columns}
              align={Alignment.Left}
            >
              {this.resourceNames}
              {this.labels}
            </ComponentSpacer>
            <EditableDescription
              description={dashboard.description}
              placeholder={`Describe ${dashboard.name}`}
              onUpdate={this.handleUpdateDescription}
            />
          </ComponentSpacer>
        </IndexList.Cell>
        {this.ownerCell}
        {this.lastModifiedCell}
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Left} stackChildren={Stack.Columns}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="Export"
              icon={IconFont.Export}
              onClick={this.handleExport}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text="Clone"
              icon={IconFont.Duplicate}
              onClick={this.handleClone}
            />
            <ConfirmationButton
              text="Delete"
              size={ComponentSize.ExtraSmall}
              onConfirm={onDeleteDashboard}
              returnValue={dashboard}
              confirmText="Confirm"
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private get ownerCell(): JSX.Element {
    const {showOwnerColumn} = this.props

    if (showOwnerColumn) {
      return <IndexList.Cell>{this.ownerName}</IndexList.Cell>
    }
  }

  private get resourceNames(): JSX.Element {
    const {dashboard} = this.props

    return (
      <EditableName
        onUpdate={this.handleUpdateDashboard}
        name={dashboard.name}
        hrefValue={`/dashboards/${dashboard.id}`}
        noNameString={DEFAULT_DASHBOARD_NAME}
      />
    )
  }

  private handleUpdateDashboard = (name: string) => {
    this.props.onUpdateDashboard({...this.props.dashboard, name})
  }

  private get labels(): JSX.Element {
    const {dashboard} = this.props

    if (!dashboard.labels.length) {
      return (
        <Label.Container
          limitChildCount={4}
          onEdit={this.handleEditLabels}
          resourceName="this Dashboard"
        />
      )
    }

    return (
      <Label.Container
        limitChildCount={4}
        className="index-list--labels"
        onEdit={this.handleEditLabels}
        resourceName="this Dashboard"
      >
        {dashboard.labels.map((label, index) => (
          <Label
            key={label.id || `label-${index}`}
            id={label.id}
            colorHex={label.properties.color}
            name={label.name}
            description={label.properties.description}
          />
        ))}
      </Label.Container>
    )
  }

  private get lastModifiedCell(): JSX.Element {
    const {dashboard} = this.props

    const relativeTimestamp = moment(dashboard.meta.updatedAt).fromNow()
    const absoluteTimestamp = moment(dashboard.meta.updatedAt).format(
      UPDATED_AT_TIME_FORMAT
    )

    return (
      <IndexList.Cell>
        <span title={absoluteTimestamp}>{relativeTimestamp}</span>
      </IndexList.Cell>
    )
  }

  private handleEditLabels = () => {
    const {dashboard, onEditLabels} = this.props
    onEditLabels(dashboard)
  }

  private get ownerName(): JSX.Element {
    const {dashboard, orgs} = this.props
    const ownerOrg = orgs.find(o => o.id === dashboard.orgID)

    return (
      <Link to={`/organizations/${dashboard.orgID}/members_tab`}>
        {ownerOrg.name}
      </Link>
    )
  }

  private handleUpdateDescription = (description: string): void => {
    const {onUpdateDashboard} = this.props
    const dashboard = {...this.props.dashboard, description}

    onUpdateDashboard(dashboard)
  }

  private handleClone = () => {
    const {onCloneDashboard, dashboard} = this.props
    onCloneDashboard(dashboard)
  }

  private handleExport = () => {
    const {onExportDashboard, dashboard} = this.props
    onExportDashboard(dashboard)
  }
}
