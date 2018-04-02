import PropTypes from 'prop-types'

const {arrayOf, number, shape, string} = PropTypes

export const annotation = shape({
  id: string.isRequired,
  startTime: string.isRequired,
  endTime: string.isRequired,
  text: string.isRequired,
  type: string.isRequired,
})

export const colorsStringSchema = arrayOf(
  shape({
    type: string.isRequired,
    hex: string.isRequired,
    id: string.isRequired,
    name: string.isRequired,
    value: string.isRequired,
  }).isRequired
)

export const colorsNumberSchema = arrayOf(
  shape({
    type: string.isRequired,
    hex: string.isRequired,
    id: string.isRequired,
    name: string.isRequired,
    value: number.isRequired,
  }).isRequired
)
