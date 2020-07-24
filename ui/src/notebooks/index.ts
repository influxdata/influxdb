import {FunctionComponent, ComponentClass, ReactNode} from 'react'
import {RemoteDataState} from 'src/types'
import {FromFluxResult} from '@influxdata/giraffe'

export interface PipeContextProps {
  children?: ReactNode
  controls?: ReactNode
}

export type PipeData = any

export interface PipeMeta {
  title: string
  visible: boolean
  loading: RemoteDataState
  error?: string
  focus: boolean
}

export interface PipeProp {
  Context:
    | FunctionComponent<PipeContextProps>
    | ComponentClass<PipeContextProps>
}

export interface FluxResult {
  source: string // the query that was used to generate the flux
  raw: string // the result from the API
  parsed: FromFluxResult // the parsed result
  error?: string // any error that might have happend while fetching
}

export type DataID<_T> = string

interface DataLookup<T> {
  [key: DataID<T>]: T
}

export interface Resource<T> {
  byID: DataLookup<T>
  allIDs: DataID<T>[]
}

export type ResourceGenerator<T> = () => T | T
export type ResourceUpdater<T> = (resource: Resource<T>) => void

export interface ResourceManipulator<T> {
  get: (id: DataID<T>) => T
  add: (id: DataID<T>, data?: T) => void
  update: (id: DataID<T>, data: Partial<T>) => void
  remove: (id: DataID<T>) => void
  indexOf: (id: DataID<T>) => number
  move: (id: DataID<T>, index: number) => void

  serialize: () => Resource<T>

  allIDs: DataID<T>[]
  all: T[]
}

export interface NotebookState {
  data: Resource<PipeData>
  meta: Resource<PipeMeta>
  readOnly?: boolean
}

export interface Notebook {
  data: ResourceManipulator<PipeData>
  meta: ResourceManipulator<PipeMeta>
  results: FluxResult
  readOnly?: boolean
}

export interface NotebookListState {
  notebooks: {
    [key: DataID<Notebook>]: Resource<NotebookState>
  }
}

export interface NotebookList {
  notebooks: {
    [key: DataID<Notebook>]: ResourceManipulator<Notebook>
  }
}

// NOTE: keep this interface as small as possible and
// don't take extending it lightly. this should only
// define what ALL pipe types require to be included
// on the page.
export interface TypeRegistration {
  type: string // a unique string that identifies a pipe
  family:
    | 'inputs'
    | 'passThrough'
    | 'test'
    | 'transform'
    | 'output'
    | 'sideEffects' // dictates grouping of related pipes
  priority?: number // 0 is lowest priority, equal priorities revert to string comparison
  disabled?: boolean // if you should show it or not
  featureFlag?: string // designates a flag that should enable the panel type
  component: FunctionComponent<PipeProp> | ComponentClass<PipeProp> // the view component for rendering the interface
  button: string // a human readable string for appending the type
  initial: any // the default state for an add
}

export interface TypeLookup {
  [key: string]: TypeRegistration
}

export const PIPE_DEFINITIONS: TypeLookup = {}

export function register(definition: TypeRegistration) {
  if (PIPE_DEFINITIONS.hasOwnProperty(definition.type)) {
    throw new Error(
      `Pipe of type [${definition.type}] has already been registered`
    )
  }

  PIPE_DEFINITIONS[definition.type] = {
    ...definition,
  }
}

// NOTE: this loads in all the modules under the current directory
// to make it easier to add new types
const context = require.context('./pipes', true, /index\.(ts|tsx)$/)
context.keys().forEach(key => {
  context(key)
})
