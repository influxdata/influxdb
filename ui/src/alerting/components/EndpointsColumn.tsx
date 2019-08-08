// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {EmptyState, ComponentSize} from '@influxdata/clockface'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

const EndpointsColumn: FunctionComponent = () => {
  return (
    <AlertsColumn
      title="Endpoints"
      testID="create-endpoint"
      onCreate={() => {}}
    >
      <EmptyState
        size={ComponentSize.ExtraSmall}
        className="alert-column--empty"
      >
        <EmptyState.Text
          text="Looks like you don’t have any Endpoints , why not create one?"
          highlightWords={['Endpoints']}
        />
      </EmptyState>
    </AlertsColumn>
  )
}

export default EndpointsColumn
