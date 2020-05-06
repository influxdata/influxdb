import * as React from 'react'
import {storiesOf} from '@storybook/react'
import {withKnobs, number, select} from '@storybook/addon-knobs'

import {Config, Plot, MAGMA} from '../src'

import {
  PlotContainer,
  xKnob,
  yKnob,
  fillKnob,
  symbolKnob,
  tableKnob,
  colorSchemeKnob,
  legendFontKnob,
  tickFontKnob,
  showAxesKnob,
  interpolationKnob,
} from './helpers'

storiesOf('XY Plot', module)
  .addDecorator(withKnobs)
  .add('Line', () => {
    const table = tableKnob()
    const colors = colorSchemeKnob()
    const legendFont = legendFontKnob()
    const tickFont = tickFontKnob()
    const x = xKnob(table)
    const y = yKnob(table)
    const fill = fillKnob(table, 'cpu')
    const interpolation = interpolationKnob()
    const showAxes = showAxesKnob()
    const lineWidth = number('Line Width', 1)
    const hoverDimension = select(
      'Hover Dimension',
      {auto: 'auto', x: 'x', y: 'y', xy: 'xy'},
      'auto'
    )

    const config: Config = {
      table,
      valueFormatters: {[y]: y => `${Math.round(y)}%`},
      legendFont,
      tickFont,
      showAxes,
      layers: [
        {
          type: 'line',
          x,
          y,
          fill,
          interpolation,
          colors,
          lineWidth,
          hoverDimension,
        },
      ],
    }

    return (
      <PlotContainer>
        <Plot config={config} />
      </PlotContainer>
    )
  })
  .add('Scatterplot', () => {
    const table = tableKnob()
    const colors = colorSchemeKnob()
    const legendFont = legendFontKnob()
    const tickFont = tickFontKnob()
    const x = xKnob(table)
    const y = yKnob(table)
    const fill = fillKnob(table, 'cpu')
    const symbol = symbolKnob(table, 'host')

    const showAxes = showAxesKnob()

    const config: Config = {
      table,
      valueFormatters: {[y]: y => `${Math.round(y)}%`},
      legendFont,
      tickFont,
      showAxes,
      layers: [
        {
          type: 'scatter',
          x,
          y,
          fill: fill,
          symbol: symbol,
          colors,
        },
      ],
    }

    return (
      <PlotContainer>
        <Plot config={config} />
      </PlotContainer>
    )
  })
  .add('Heatmap', () => {
    const table = tableKnob()
    const colors = colorSchemeKnob(MAGMA)
    const legendFont = legendFontKnob()
    const tickFont = tickFontKnob()
    const x = xKnob(table)
    const y = yKnob(table)
    const showAxes = showAxesKnob()

    const config: Config = {
      table,
      legendFont,
      tickFont,
      showAxes,
      valueFormatters: {[y]: y => `${Math.round(y)}%`},
      layers: [{type: 'heatmap', x, y, colors}],
    }

    return (
      <PlotContainer>
        <Plot config={config} />
      </PlotContainer>
    )
  })
  .add('Histogram', () => {
    const table = tableKnob()
    const colors = colorSchemeKnob()
    const legendFont = legendFontKnob()
    const tickFont = tickFontKnob()
    const x = xKnob(table, '_value')
    const showAxes = showAxesKnob()
    const binCount = number('Bin Count', 10)

    const config: Config = {
      table,
      legendFont,
      tickFont,
      showAxes,
      valueFormatters: {[x]: x => `${Math.round(x)}%`},
      layers: [{type: 'histogram', x, fill: ['cpu'], colors, binCount}],
    }

    return (
      <PlotContainer>
        <Plot config={config} />
      </PlotContainer>
    )
  })
