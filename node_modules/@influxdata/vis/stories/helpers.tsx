import * as React from 'react'
import {select, text, boolean} from '@storybook/addon-knobs'

import {Table} from '../src'
import {CPU} from './data'
import * as colorSchemes from '../src/constants/colorSchemes'

export const PlotContainer = ({children}) => (
  <div
    style={{
      width: 'calc(100vw - 100px)',
      height: 'calc(100vh - 100px)',
      margin: '50px',
    }}
  >
    {children}
  </div>
)

const multiSelect = (
  label: string,
  options: string[],
  defaultValue: string
): string[] => {
  const values = []
  const defaultValues = [defaultValue]

  options.forEach((value: string) => {
    const checkboxLabel = `${label}: ${value}`

    const selected = boolean(checkboxLabel, defaultValues.includes(value))

    if (selected) {
      values.push(value)
    }
  })

  return values
}

export const tickFontKnob = (initial?: string) =>
  text('Tick Font', initial || '10px sans-serif')

export const legendFontKnob = (initial?: string) =>
  text('Legend Font', initial || '12px sans-serif')

export const colorSchemeKnob = (initial?: string[]) =>
  select(
    'Color Scheme',
    {
      'Nineteen Eighty Four': colorSchemes.NINETEEN_EIGHTY_FOUR,
      Atlantis: colorSchemes.ATLANTIS,
      'Do Androids Dream': colorSchemes.DO_ANDROIDS_DREAM,
      Delorean: colorSchemes.DELOREAN,
      Cthulhu: colorSchemes.CTHULHU,
      Ectoplasm: colorSchemes.ECTOPLASM,
      'T Max 400 Film': colorSchemes.T_MAX_400_FILM,
      Viridis: colorSchemes.VIRIDIS,
      Magma: colorSchemes.MAGMA,
      Inferno: colorSchemes.INFERNO,
      Plasma: colorSchemes.PLASMA,
      ylOrRd: colorSchemes.YL_OR_RD,
      ylGnBu: colorSchemes.YL_GN_BU,
      buGn: colorSchemes.BU_GN,
    },
    initial || colorSchemes.NINETEEN_EIGHTY_FOUR
  )

export const tableKnob = (initial?: Table) =>
  select('Data', {CPU}, initial || CPU)

/*
  Find all column keys in a table suitable for mapping to the `x` or `y`
  aesthetic, and retun as a map from column keys to column names.
*/
const findXYColumns = (table: Table) =>
  table.columnKeys.reduce((acc, k) => {
    const columnType = table.getColumnType(k)

    if (columnType !== 'number' && columnType !== 'time') {
      return acc
    }

    return {
      ...acc,
      [k]: table.getColumnName(k),
    }
  }, {})

const findStringColumns = (table: Table) =>
  table.columnKeys.filter(k => table.getColumnType(k) === 'string')

export const xKnob = (table: Table, initial?: string) =>
  select('x', findXYColumns(table), initial || '_time')

export const yKnob = (table: Table, initial?: string) =>
  select('y', findXYColumns(table), initial || '_value')

export const fillKnob = (table: Table, initial?: string) =>
  multiSelect('fill', findStringColumns(table), initial || 'cpu')

export const symbolKnob = (table: Table, initial?: string) =>
  multiSelect('symbol', findStringColumns(table), initial || 'host')

export const interpolationKnob = () =>
  select(
    'Interpolation',
    {
      linear: 'linear',
      monotoneX: 'monotoneX',
      monotoneY: 'monotoneY',
      cubic: 'cubic',
      step: 'step',
      stepBefore: 'stepBefore',
      stepAfter: 'stepAfter',
      natural: 'natural',
    },
    'monotoneX'
  )

export const showAxesKnob = () => boolean('Axes', true)
