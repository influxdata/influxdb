import {
  TasksApi,
  UsersApi,
  DashboardsApi,
  CellsApi,
  TelegrafsApi,
  AuthorizationsApi,
} from 'src/api'

const basePath = '/api/v2'

export const taskAPI = new TasksApi({basePath})
export const usersAPI = new UsersApi({basePath})
export const dashboardsAPI = new DashboardsApi({basePath})
export const cellsAPI = new CellsApi({basePath})
export const telegrafsAPI = new TelegrafsApi({basePath})
export const authorizationsAPI = new AuthorizationsApi({basePath})
