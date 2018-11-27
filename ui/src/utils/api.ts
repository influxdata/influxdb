import {TasksApi, UsersApi, DashboardsApi, CellsApi} from 'src/api'

export const taskAPI = new TasksApi({basePath: '/api/v2'})
export const usersAPI = new UsersApi({basePath: '/api/v2'})
export const dashboardsAPI = new DashboardsApi({basePath: '/api/v2'})
export const cellsAPI = new CellsApi({basePath: '/api/v1'})
