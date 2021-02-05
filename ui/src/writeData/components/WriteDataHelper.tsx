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
  Icon,
  IconFont,
  ComponentSize,
} from '@influxdata/clockface'
import WriteDataHelperTokens from 'src/writeData/components/WriteDataHelperTokens'
import WriteDataHelperBuckets from 'src/writeData/components/WriteDataHelperBuckets'

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
    <Panel backgroundColor={InfluxColors.Castle}>
      <Panel.Header size={ComponentSize.ExtraSmall}>
        <div
          className={`write-data-helper--heading write-data-helper--heading__${mode}`}
          onClick={handleToggleClick}
        >
          <Icon
            glyph={IconFont.CaretRight}
            className="write-data-helper--caret"
          />
          <Heading
            element={HeadingElement.H5}
            weight={FontWeight.Regular}
            selectable={true}
          >
            Code Sample Options
          </Heading>
        </div>
      </Panel.Header>
      {mode === 'expanded' && (
        <Panel.Body size={ComponentSize.ExtraSmall}>
          <p>
            Control how code samples in the documentation are populated with
            system resources. Not all code samples make use of system resources.
          </p>
          <Grid>
            <Grid.Row>
              <Grid.Column widthSM={Columns.Six}>
                <WriteDataHelperTokens />
              </Grid.Column>
              <Grid.Column widthSM={Columns.Six}>
                <WriteDataHelperBuckets />
              </Grid.Column>
            </Grid.Row>
          </Grid>
        </Panel.Body>
      )}
    </Panel>
  )
}

export default WriteDataHelper
