import React, {FC} from 'react'

import {Page, Grid, Columns} from '@influxdata/clockface'

import UsageToday from './Usage/UsageToday'

interface Props {
  history: string
  limitStatuses: string
  selectedRange: string
  accountType: string
  billingStart: string
}


const Usage: FC<Props> = (props) => {
    const {
      history,
      limitStatuses,
      selectedRange,
      accountType,
      billingStart,
    } = props

    return (
      <Page titleTag="Usage">
        <Page.Contents scrollable={true}>
          <Grid>
            <Grid.Row>
              <Grid.Column widthXS={Columns.Twelve}>
                <UsageToday
                  history={history}
                  limitStatuses={limitStatuses}
                  selectedRange={selectedRange}
                  accountType={accountType}
                  billingStart={billingStart}
                />
              </Grid.Column>
            </Grid.Row>
          </Grid>
        </Page.Contents>
      </Page>
    )
  }
}

export default Usage
