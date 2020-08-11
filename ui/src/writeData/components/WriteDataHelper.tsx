// Libraries
import React, {FC, useState} from 'react'

// Components
import {
  Panel,
  InfluxColors,
  Heading,
  HeadingElement,
  FontWeight,
  Grid,
  Columns,
} from '@influxdata/clockface'
import WriteDataHelperInfo from 'src/writeData/components/WriteDataHelperInfo'
import WriteDataHelperTokens from 'src/writeData/components/WriteDataHelperTokens'
import WriteDataHelperBuckets from 'src/writeData/components/WriteDataHelperBuckets'
import GetResources from 'src/resources/components/GetResources'

// Types
import {ResourceType} from 'src/types'

const WriteDataHelper: FC<{}> = () => {
  const [mode, changeMode] = useState<'expanded' | 'collapsed'>('expanded')

  const handleToggleClick = (): void => {
    if (mode === 'expanded') {
      changeMode('collapsed')
    } else {
      changeMode('expanded')
    }
  }

  return (
    <GetResources
      resources={[ResourceType.Authorizations, ResourceType.Buckets]}
    >
      <Panel backgroundColor={InfluxColors.Castle}>
        <Panel.Header>
          <Heading element={HeadingElement.H4} weight={FontWeight.Regular}>
            Resources
            <button onClick={handleToggleClick}>Toggle</button>
          </Heading>
        </Panel.Header>
        {mode === 'expanded' && (
          <Panel.Body>
            <Grid>
              <Grid.Row>
                <Grid.Column widthSM={Columns.Four}>
                  <WriteDataHelperInfo />
                </Grid.Column>
                <Grid.Column widthSM={Columns.Four}>
                  <WriteDataHelperTokens />
                </Grid.Column>
                <Grid.Column widthSM={Columns.Four}>
                  <WriteDataHelperBuckets />
                </Grid.Column>
              </Grid.Row>
            </Grid>
          </Panel.Body>
        )}
      </Panel>
    </GetResources>
  )
}

export default WriteDataHelper
