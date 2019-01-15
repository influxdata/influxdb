// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

// Components
import {
  Button,
  IconFont,
  ComponentSize,
  ComponentSpacer,
  IndexList,
  ConfirmationButton,
  Stack,
  Label,
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

interface Props {
  dashboard: Dashboard
  orgs: Organization[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onEditLabels: (dashboard: Dashboard) => void
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
              <Link className={this.nameClassName} to={`/dashboards/${id}`}>
                {this.name}
              </Link>
              {this.labels}
            </ComponentSpacer>
            <EditableDescription
              description={dashboard.description}
              placeholder={`Describe ${dashboard.name}`}
              onUpdate={this.handleUpdateDescription}
            />
          </ComponentSpacer>
        </IndexList.Cell>
        <IndexList.Cell>{this.ownerName}</IndexList.Cell>
        {this.lastModifiedCell}
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Left} stackChildren={Stack.Columns}>
            <Button
              size={ComponentSize.ExtraSmall}
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

  private get labels(): JSX.Element {
    const {dashboard} = this.props

    if (!dashboard.labels.length) {
      return (
        <Label.Container
          limitChildCount={4}
          className="index-list--labels"
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
        {dashboard.labels.map(label => (
          <Label
            key={`${label.resourceID}-${label.name}`}
            id={label.resourceID}
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

  private get name(): string {
    const {dashboard} = this.props

    return dashboard.name || DEFAULT_DASHBOARD_NAME
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

  private get nameClassName(): string {
    const {dashboard} = this.props

    const dashboardIsUntitled =
      dashboard.name === '' || dashboard.name === DEFAULT_DASHBOARD_NAME

    return classnames('index-list--resource-name', {
      'untitled-name': dashboardIsUntitled,
    })
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
}
