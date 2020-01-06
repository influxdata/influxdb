// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {
  Panel,
  FlexBox,
  FlexDirection,
  ComponentSize,
  AlignItems,
  Grid,
  Columns,
  Page,
} from '@influxdata/clockface'
import Resources from 'src/me/components/Resources'
import Header from 'src/me/components/UserPageHeader'
import Docs from 'src/me/components/Docs'
import GettingStarted from 'src/me/components/GettingStarted'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  me: AppState['me']
  orgName: string
}

@ErrorHandling
export class MePage extends PureComponent<StateProps> {
  public render() {
    const {me, orgName} = this.props

    return (
      <Page titleTag={pageTitleSuffixer(['Home'])}>
        <Header userName={me.name} orgName={orgName} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <Grid>
            <Grid.Row>
              <Grid.Column widthSM={Columns.Eight} widthMD={Columns.Nine}>
                <FlexBox
                  direction={FlexDirection.Column}
                  margin={ComponentSize.Small}
                  alignItems={AlignItems.Stretch}
                  stretchToFitWidth={true}
                  testID="getting-started"
                >
                  <Panel>
                    <Panel.Header>
                      <h4>Getting started with InfluxDB 2.0</h4>
                    </Panel.Header>
                    <Panel.Body>
                      <GettingStarted />
                    </Panel.Body>
                  </Panel>
                  <Docs />
                </FlexBox>
              </Grid.Column>
              <Grid.Column widthSM={Columns.Four} widthMD={Columns.Three}>
                <Resources me={me} />
              </Grid.Column>
            </Grid.Row>
          </Grid>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const org = getOrg(state)
  const {me} = state

  return {me, orgName: get(org, 'name', '')}
}

export default connect<StateProps>(
  mstp,
  null
)(MePage)
