// Libraries
import React, {FC} from 'react'

// Components
import AddButtons from 'src/notebooks/components/AddButtons'
import {FlexBox, JustifyContent, ComponentSize} from '@influxdata/clockface'

// Styles
import 'src/notebooks/components/EmptyPipeList.scss'

const EmptyPipeList: FC = () => {
  return (
    <div className="notebook-empty">
      <div className="notebook-empty--graphic" />
      <h3>Welcome to Flows</h3>
      <p>
        This is a more flexible way to explore, visualize, and (eventually)
        alert on your data
      </p>
      <p>
        Get started by <strong>Adding a Cell</strong> below
      </p>
      <FlexBox
        justifyContent={JustifyContent.Center}
        margin={ComponentSize.Medium}
        className="notebook-empty--buttons"
      >
        <AddButtons />
      </FlexBox>
    </div>
  )
}

export default EmptyPipeList
