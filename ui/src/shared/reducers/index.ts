import app from 'src/shared/reducers/app'
import links from 'src/shared/reducers/links'
import currentDashboard from 'src/shared/reducers/currentDashboard'
import {notificationsReducer} from 'src/shared/reducers/notifications'

export default {
  app,
  links,
  currentDashboard,
  notifications: notificationsReducer,
}
