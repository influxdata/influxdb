// Libraries
import React, {FC} from 'react'

// Components
import {
  Panel,
  InfluxColors,
  Heading,
  FontWeight,
  HeadingElement,
} from '@influxdata/clockface'

const TelegrafPluginsExplainer: FC = () => {
  return (
    <Panel backgroundColor={InfluxColors.Castle} style={{marginBottom: '8px'}}>
      <Panel.Header>
        <Heading element={HeadingElement.H4} weight={FontWeight.Regular}>
          Getting Started with Telegraf
        </Heading>
      </Panel.Header>
      <Panel.Body>
        <p>
          Telegraf is InfluxData’s data collection agent for collecting and
          reporting metrics. Its vast library of input plugins and
          “plug-and-play” architecture lets you quickly and easily collect
          metrics from many different sources.
        </p>
        <p>
          You will need to have Telegraf installed in order to use this plugin.
          See our handy{' '}
          <a
            href="https://docs.influxdata.com/telegraf/v1.15/introduction/installation/"
            target="_blank"
          >
            Installation Guide
          </a>
        </p>
      </Panel.Body>
    </Panel>
  )
}

export default TelegrafPluginsExplainer
