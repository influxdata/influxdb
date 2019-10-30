import app from 'src/shared/reducers/app'
import links from 'src/shared/reducers/links'
import {notificationsReducer} from 'src/shared/reducers/notifications'
import {predicatesReducer} from 'src/shared/reducers/predicates'

export default {
  app,
  links,
  notifications: notificationsReducer,
  predicates: predicatesReducer,
}
