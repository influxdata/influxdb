// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {
  Button,
  IconFont,
  ComponentSize,
  ComponentSpacer,
  IndexList,
  ConfirmationButton,
  Stack,
} from 'src/clockface'

// Types
import {Dashboard} from 'src/types/v2'
import {Alignment} from 'src/clockface'
import moment from 'moment'

// Constants
import {
  UPDATED_AT_TIME_FORMAT,
  DEFAULT_DASHBOARD_NAME,
} from 'src/dashboards/constants'

interface Props {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
}

export default class DashboardsIndexTableRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onDeleteDashboard} = this.props
    const {id} = dashboard

    return (
      <IndexList.Row key={`dashboard-id--${id}`} disabled={false}>
        <IndexList.Cell>
          <Link className={this.nameClassName} to={`/dashboards/${id}`}>
            {this.name}
          </Link>
        </IndexList.Cell>
        <IndexList.Cell>You</IndexList.Cell>
        <IndexList.Cell>
          {moment(dashboard.meta.updatedAt).format(UPDATED_AT_TIME_FORMAT)}
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Left} stackChildren={Stack.Columns}>
            <Button
              size={ComponentSize.ExtraSmall}
              text="Export"
              icon={IconFont.Export}
              onClick={this.handleExport}
            />
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

  private get name(): string {
    const {dashboard} = this.props

    return dashboard.name || DEFAULT_DASHBOARD_NAME
  }

  private get nameClassName(): string {
    const {dashboard} = this.props

    if (dashboard.name === '' || dashboard.name === DEFAULT_DASHBOARD_NAME) {
      return 'untitled-name'
    }
  }

  private handleExport = () => {
    const {onExportDashboard, dashboard} = this.props
    onExportDashboard(dashboard)
  }

  private handleClone = () => {
    const {onCloneDashboard, dashboard} = this.props
    onCloneDashboard(dashboard)
  }
}
