import {register} from 'src/notebooks'
import View from './view'

register({
  type: 'test',
  family: 'test',
  component: View,
  priority: -1,
  featureFlag: 'notebook-panel--test-flux',
  button: 'Flux Result Tester',
  initial: {
    panelVisibility: 'visible',
    panelHeight: 200,
    properties: {
      type: 'xy',
      position: 'overlaid',
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
      xColumn: '_time',
      yColumn: 'state',
      shadeBelow: true,
      geom: 'monotoneX',
      shape: 'chronograf-v2',
    },
  },
})
