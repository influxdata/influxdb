import React from 'react'

// Components
import {Page} from '@influxdata/clockface'
import FlowHeader from 'src/notebooks/components/header'
import PipeList from 'src/notebooks/components/PipeList'
import MiniMap from 'src/notebooks/components/minimap/MiniMap'

const FlowPage = () => {
  return (
    <Page titleTag="Flows">
      <FlowHeader />
      <Page.Contents
        fullWidth={true}
        scrollable={false}
        className="notebook-page"
      >
        <div className="notebook">
          <MiniMap />
          <PipeList />
        </div>
      </Page.Contents>
    </Page>
  )
}

export default FlowPage
