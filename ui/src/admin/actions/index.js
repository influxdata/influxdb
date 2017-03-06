import {getUsers, getRoles} from 'src/admin/apis'
import {killQuery as killQueryProxy} from 'shared/apis/metaQuery'

export const loadUsers = ({users}) => ({
  type: 'LOAD_USERS',
  payload: {
    users,
  },
})

export const loadRoles = ({roles}) => ({
  type: 'LOAD_ROLES',
  payload: {
    roles,
  },
})

export const killQuery = (queryID) => ({
  type: 'KILL_QUERY',
  payload: {
    queryID,
  },
})

export const setQueryToKill = (queryIDToKill) => ({
  type: 'SET_QUERY_TO_KILL',
  payload: {
    queryIDToKill,
  },
})

export const loadQueries = (queries) => ({
  type: 'LOAD_QUERIES',
  payload: {
    queries,
  },
})

// async actions
export const loadUsersAsync = (url) => async (dispatch) => {
  const {data} = await getUsers(url)
  dispatch(loadUsers(data))
}

export const loadRolesAsync = (url) => async (dispatch) => {
  const {data} = await getRoles(url)
  dispatch(loadRoles(data))
}

export const killQueryAsync = (source, queryID) => (dispatch) => {
  // optimistic update
  dispatch(killQuery(queryID))
  dispatch(setQueryToKill(null))

  // kill query on server
  killQueryProxy(source, queryID)
}
