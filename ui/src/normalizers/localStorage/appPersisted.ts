// Types
import {LocalStorage} from 'src/types'

// Constants
import {timeZones} from 'src/types/timeZones'

type AppPersisted = LocalStorage['app']['persisted']

export const normalizeApp = (app: LocalStorage['app']) => {
  return {...app, persisted: normalizeAppPersisted(app.persisted)}
}

export const normalizeAppPersisted = (
  persisted: AppPersisted
): AppPersisted => {
  return {...persisted, timeZone: normalizeTimeZone(persisted.timeZone)}
}

const normalizeTimeZone = timeZone => {
  const validTimeZone = timeZones.find(tz => tz === timeZone)
  if (!validTimeZone) {
    return 'Local'
  }

  return timeZone
}
