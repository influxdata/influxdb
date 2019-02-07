// Libraries
import React, {PureComponent} from 'react'
import moment from 'moment'

// Components
import {
  IndexList,
  Alignment,
  ComponentSize,
  ConfirmationButton,
  ComponentSpacer,
  Stack,
  Button,
  IconFont,
  ComponentColor,
} from 'src/clockface'

// Types
import {Dashboard} from 'src/types/v2'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'
import EditableName from 'src/shared/components/EditableName'

interface Props {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
}

export default class DashboardRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onDeleteDashboard} = this.props

    return (
      <IndexList.Row key={dashboard.id}>
        <IndexList.Cell>
          <EditableName
            onUpdate={this.handleUpdateDashboard}
            name={dashboard.name}
            hrefValue={`/dashboards/${dashboard.id}`}
          />
        </IndexList.Cell>
        {this.lastModifiedCell}
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ComponentSpacer stackChildren={Stack.Columns} align={Alignment.Left}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text="Clone"
              icon={IconFont.Duplicate}
              titleText="Create a duplicate copy of this Dashboard"
              onClick={this.handleCloneDashboard}
            />
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={onDeleteDashboard}
              returnValue={dashboard}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleUpdateDashboard = (name: string) => {
    this.props.onUpdateDashboard({...this.props.dashboard, name})
  }

  private handleCloneDashboard = (): void => {
    const {dashboard, onCloneDashboard} = this.props

    onCloneDashboard(dashboard)
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
}
