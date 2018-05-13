import {TEMP_VAR_INTERVAL} from 'src/shared/constants'

const groupByTimes = [
  {defaultTimeBound: TEMP_VAR_INTERVAL, seconds: 604800, menuOption: 'auto'},
  {defaultTimeBound: 'now() - 5m', seconds: 10, menuOption: '10s'},
  {defaultTimeBound: 'now() - 15m', seconds: 60, menuOption: '1m'},
  {defaultTimeBound: 'now() - 1h', seconds: 300, menuOption: '5m'},
  {defaultTimeBound: 'now() - 6h', seconds: 600, menuOption: '10m'},
  {defaultTimeBound: 'now() - 12h', seconds: 1800, menuOption: '30m'},
  {defaultTimeBound: 'now() - 24h', seconds: 3600, menuOption: '1h'},
  {defaultTimeBound: 'now() - 2d', seconds: 21600, menuOption: '6h'},
  {defaultTimeBound: 'now() - 7d', seconds: 86400, menuOption: '1d'},
  {defaultTimeBound: 'now() - 30d', seconds: 604800, menuOption: '7d'},
]

export default groupByTimes
