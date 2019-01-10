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
  Label,
} from 'src/clockface'
import EditableDescription from 'src/shared/components/editable_description/EditableDescription'

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
  onUpdateDashboard: (dashboard: Dashboard) => void
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
        <IndexList.Cell>Owner does not come back from API</IndexList.Cell>
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
      return
    }

    return (
      <Label.Container limitChildCount={4} className="index-list--labels">
        {dashboard.labels.map(label => (
          <Label
            key={label.resourceID}
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
