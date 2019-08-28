// Libraries
import React, {SFC} from 'react'

// Components
import {Panel, InfluxColors} from '@influxdata/clockface'

const TelegrafExplainer: SFC = () => (
  <Panel backgroundColor={InfluxColors.Onyx} style={{marginTop: '32px'}}>
    <Panel.Header>
      <Panel.Title>What is Telegraf?</Panel.Title>
    </Panel.Header>
    <Panel.Body>
      <p>
        Telegraf is an agent written in Go for collecting metrics and writing
        them into <strong>InfluxDB</strong> or other possible outputs.
        <br />
        <br />
        Here's a handy guide for{' '}
        <a
          href="https://v2.docs.influxdata.com/v2.0/write-data/use-telegraf/"
          target="_blank"
        >
          Getting Started with Telegraf
        </a>
      </p>
    </Panel.Body>
  </Panel>
)

export default TelegrafExplainer
