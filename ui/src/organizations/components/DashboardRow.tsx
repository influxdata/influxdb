// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'

// Components
import {IndexList, Alignment, ComponentSize} from 'src/clockface'
import ConfirmationButton from 'src/clockface/components/confirmation_button/ConfirmationButton'

// Types
import {Dashboard} from 'src/types/v2'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface Props {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
}

export default class DashboardRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onDeleteDashboard} = this.props

    return (
      <IndexList.Row key={dashboard.id}>
        <IndexList.Cell>
          <Link to={`/dashboards/${dashboard.id}`}>{dashboard.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell revealOnHover={true}>
          {moment(dashboard.meta.updatedAt).format(UPDATED_AT_TIME_FORMAT)}
        </IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ConfirmationButton
            size={ComponentSize.ExtraSmall}
            text="Delete"
            confirmText="Confirm"
            onConfirm={onDeleteDashboard}
            returnValue={dashboard}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }
}
