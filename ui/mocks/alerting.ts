import {
  Check,
  CheckType,
  DashboardQuery,
  QueryEditMode,
  CheckBase,
} from 'src/types'

export const query: DashboardQuery = {
  text: 'this is query',
  editMode: QueryEditMode.Advanced,
  builderConfig: null,
  name: 'great q',
}

export const check: Check = {
  id: '1',
  type: CheckType.Threshold,
  name: 'Amoozing check',
  orgID: 'lala',
  createdAt: new Date('December 17, 2019'),
  updatedAt: new Date('April 17, 2019'),
  query: query,
  status: CheckBase.StatusEnum.Active,
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
}
