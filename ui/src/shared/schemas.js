import {PropTypes} from 'react'

const {shape, string} = PropTypes

export const annotation = shape({
  id: string.isRequired,
  startTime: string.isRequired,
  endTime: string.isRequired,
  text: string.isRequired,
  type: string.isRequired,
})
