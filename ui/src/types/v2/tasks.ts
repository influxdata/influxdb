import {Organization} from 'src/types/v2/orgs'
import {Task as TaskAPI} from 'src/api'

export interface Task extends TaskAPI {
  organization: Organization
}
