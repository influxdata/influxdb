// Libraries
import React, {FC} from 'react'

// Components
import {Grid, Input, Form} from '@influxdata/clockface'

// Types
import {CheckTagSet} from 'src/types'

interface Props {
  index: number
  tagSet: CheckTagSet
  handleChangeTagRow: (i: number, tags: CheckTagSet) => void
}

const CheckTagRow: FC<Props> = ({tagSet, handleChangeTagRow, index}) => {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    handleChangeTagRow(index, {...tagSet, [e.target.name]: e.target.value})
  }
  return (
    <Grid>
      <Grid.Row>
        <Grid.Column widthSM={6}>
          <Form.Element label="Tag Key">
            <Input
              name="key"
              onChange={handleChange}
              titleText="Name of the check"
              value={tagSet.key}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={6}>
          <Form.Element label="Tag Value">
            <Input
              name="value"
              onChange={handleChange}
              titleText="Offset check interval"
              value={tagSet.value}
            />
          </Form.Element>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  )
}

export default CheckTagRow
