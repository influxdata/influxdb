import {TimeZone} from 'src/types'

export const TIME_ZONES: Array<{timeZone: TimeZone; displayName: string}> = [
  {timeZone: 'Local', displayName: 'Local Time'},
  {timeZone: 'UTC', displayName: 'UTC'},

  // We can expose more time zone settings in the future, if we want:
  //
  //     {timeZone: 'America/Los_Angeles', displayName: 'Los Angeles'}
  //
]
