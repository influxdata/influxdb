// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Panel, InfluxColors, ComponentSize} from '@influxdata/clockface'
import {TextAlignProperty} from 'csstype'

interface Props {
  textAlign?: TextAlignProperty
  titleSize?: ComponentSize
  bodySize?: ComponentSize
}

const TelegrafExplainer: FunctionComponent<Props> = ({
  textAlign = 'inherit',
  titleSize,
  bodySize,
}) => (
  <Panel
    backgroundColor={InfluxColors.Smoke}
    style={{textAlign, marginTop: '32px'}}
  >
    <Panel.Header>
      <Panel.Title size={titleSize}>What is Telegraf?</Panel.Title>
    </Panel.Header>
    <Panel.Body size={bodySize}>
      Telegraf is an agent written in Go for collecting metrics and writing them
      into <strong>InfluxDB</strong> or other possible outputs.
      <br />
      <br />
      Here's a handy guide for{' '}
      <a
        href="https://v2.docs.influxdata.com/v2.0/write-data/use-telegraf/"
        target="_blank"
      >
        Getting Started with Telegraf
      </a>
    </Panel.Body>
  </Panel>
)

export default TelegrafExplainer
