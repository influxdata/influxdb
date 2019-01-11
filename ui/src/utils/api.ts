import {
  TasksApi,
  UsersApi,
  DashboardsApi,
  CellsApi,
  TelegrafsApi,
  AuthorizationsApi,
  ViewsApi,
  WriteApi,
  SourcesApi,
  DefaultApi,
  BucketsApi,
  OrganizationsApi,
  QueryApi,
  SetupApi,
} from 'src/api'

const basePath = '/api/v2'

export const baseAPI = new DefaultApi({basePath})
export const viewsAPI = new ViewsApi({basePath})
export const taskAPI = new TasksApi({basePath})
export const usersAPI = new UsersApi({basePath})
export const dashboardsAPI = new DashboardsApi({basePath})
export const cellsAPI = new CellsApi({basePath})
export const telegrafsAPI = new TelegrafsApi({basePath})
export const authorizationsAPI = new AuthorizationsApi({basePath})
export const writeAPI = new WriteApi({basePath})
export const sourcesAPI = new SourcesApi({basePath})
export const bucketsAPI = new BucketsApi({basePath})
export const orgsAPI = new OrganizationsApi({basePath})
export const queryAPI = new QueryApi({basePath})
export const setupAPI = new SetupApi({basePath})
