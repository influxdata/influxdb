import {Action, ActionTypes} from 'src/shared/actions/links'
import {Links} from 'src/types/links'

const initialState: Links = {
  authorizations: '',
  buckets: '',
  dashboards: '',
  external: {
    statusFeed: '',
  },
  query: {
    self: '',
    ast: '',
    suggestions: '',
  },
  orgs: '',
  setup: '',
  signin: '',
  signout: '',
  sources: '',
  system: {debug: '', health: '', metrics: ''},
  tasks: '',
  users: '',
  write: '',
  variables: '',
  views: '',
  defaultDashboard: '',
  me: '',
}

const linksReducer = (state = initialState, action: Action): Links => {
  switch (action.type) {
    case ActionTypes.LinksGetCompleted: {
      const {links} = action.payload
      return {...links, defaultDashboard: '/v2/dashboards/029d13fda9c5b000'}
    }
  }

  return state
}

export default linksReducer
