/*
import { TimeRange } from 'src/types/queries'
import { DashboardQuery } from 'src/client'

export interface QueryPipe {
    type: 'query'
    queries: DashboardQuery[]
}

export enum ViewTypes {
    'raw',
    'graph'
}

export interface VisualizationPipe {
    type: 'visualization'
    viewType: ViewTypes
    viewOptions?: any           // this should be dependant based on which viewType was selected
}

export type NetworkPipe =
    | QueryPipe
    | VisualizationPipe
    | TaskPipe
    | NotificationPipe

export interface Notebook {
    id?: string
    timeRange?: TimeRange // pretty sure this doesn't belong here
    pipes: NotebookPipe[]
}

*/
