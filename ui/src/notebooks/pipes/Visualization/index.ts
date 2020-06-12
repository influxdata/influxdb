import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'visualization',
  component: View,
  button: 'Visualization',
  initial: {
    panelVisibility: 'visible',
    panelHeight: 200,
    properties: {
      type: 'xy',
      position: 'overlaid',
      legend: {},
      note: '',
      showNoteWhenEmpty: false,
      axes: {
        x: {
          bounds: ['', ''],
          label: '',
          prefix: '',
          suffix: '',
          base: '10',
          scale: 'linear',
        },
        y: {
          bounds: ['', ''],
          label: '',
          prefix: '',
          suffix: '',
          base: '10',
          scale: 'linear',
        },
      },
      geom: 'line',
      shape: 'chronograf-v2',
    },
  },
})
