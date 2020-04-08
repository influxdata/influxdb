import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {ResourceType, Dashboard, Task, Variable} from 'src/types'

export type DashboardSortKey = keyof Dashboard | 'meta.updatedAt'
export type TaskSortKey = keyof Task
export type VariableSortKey = keyof Variable | 'arguments.type'

export type SortKey = DashboardSortKey | TaskSortKey | VariableSortKey

export interface SortDropdownItem {
  label: string
  sortKey: SortKey
  sortType: SortTypes
  sortDirection: Sort
}

export const generateSortItems = (
  resourceType: ResourceType
): SortDropdownItem[] => {
  switch (resourceType) {
    case ResourceType.Dashboards:
      return [
        {
          label: 'Name (A → Z)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Name (Z → A)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
        {
          label: 'Modified (Oldest)',
          sortKey: 'meta.updatedAt',
          sortType: SortTypes.Date,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Modified (Newest)',
          sortKey: 'meta.updatedAt',
          sortType: SortTypes.Date,
          sortDirection: Sort.Descending,
        },
      ]
    case ResourceType.Tasks:
      return [
        {
          label: 'Name (A → Z)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Name (Z → A)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
        {
          label: 'Active',
          sortKey: 'status',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Inactive',
          sortKey: 'status',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
        {
          label: 'Completed (Oldest)',
          sortKey: 'latestCompleted',
          sortType: SortTypes.Date,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Completed (Newest)',
          sortKey: 'latestCompleted',
          sortType: SortTypes.Date,
          sortDirection: Sort.Descending,
        },
        {
          label: 'Schedule (Most Often)',
          sortKey: 'every',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Schedule (Least Often)',
          sortKey: 'every',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
      ]
    case ResourceType.Variables:
      return [
        {
          label: 'Name (A → Z)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Name (Z → A)',
          sortKey: 'name',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
        {
          label: 'Type (A → Z)',
          sortKey: 'arguments.type',
          sortType: SortTypes.String,
          sortDirection: Sort.Ascending,
        },
        {
          label: 'Type (Z → A)',
          sortKey: 'arguments.type',
          sortType: SortTypes.String,
          sortDirection: Sort.Descending,
        },
      ]
  }
}
