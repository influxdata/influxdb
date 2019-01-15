import {Task as TaskAPI, Organization} from 'src/api'

export interface Task extends TaskAPI {
  organization: Organization
}
