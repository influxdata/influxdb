// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

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
import Docs from 'src/me/components/Docs'
import GettingStarted from 'src/me/components/GettingStarted'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

@ErrorHandling
export class MePage extends PureComponent<Props> {
  public render() {
    const {me} = this.props

    return (
      <Page titleTag={pageTitleSuffixer(['Home'])}>
        <Page.Header fullWidth={false}>
          <Page.Title title="Getting Started" testID="home-page--header" />
          <RateLimitAlert />
        </Page.Header>
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

const mstp = (state: AppState) => {
  const {me} = state

  return {me}
}

const connector = connect(mstp)

export default connector(MePage)
