import {Action, ActionTypes} from 'src/shared/actions/links'
import {Links} from 'src/types/v2/links'

const initialState: Links = {
  auths: '',
  buckets: '',
  signin: '',
  signout: '',
  dashboards: '',
  external: {
    statusFeed: '',
  },
  flux: {
    ast: '',
    self: '',
    suggestions: '',
  },
  orgs: '',
  query: '',
  setup: '',
  sources: '',
  system: {debug: '', health: '', metrics: ''},
  tasks: '',
  users: '',
  write: '',
  defaultDashboard: '',
}

const linksReducer = (state = initialState, action: Action): Links => {
  switch (action.type) {
    case ActionTypes.LinksGetCompleted: {
      const {links} = action.payload
      return {...links, defaultDashboard: '/v2/dashboards/029d13fda9c5b000'}
    }

    case ActionTypes.SetDefaultDashboardLink: {
      const {defaultDashboard} = action.payload
      return {...state, defaultDashboard}
    }
  }

  return state
}

export default linksReducer
