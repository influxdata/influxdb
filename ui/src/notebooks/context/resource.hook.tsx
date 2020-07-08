import {useCallback} from 'react'
import {Resource, DataID} from 'src/notebooks'

type Generator<T> = () => T | T
type ResourceUpdater<T> = (resource: Resource<T>) => void
interface ResourceManipulator<T> {
  get: (id: DataID<T>) => T
  add: (id: DataID<T>, data?: T) => void
  update: (id: DataID<T>, data: Partial<T>) => void
  remove: (id: DataID<T>) => void
  indexOf: (id: DataID<T>) => number
  move: (id: DataID<T>, index: number) => number

  allIDs: DataID<T>[]
  all: T[]
}

function useResource<T>(
  resource: Resource<T>,
  onChange: ResourceUpdater<T>,
  generator?: Generator<T>
): ResourceManipulator<T> {
  return {
    get: useCallback(
      (id: DataID<T>): T => {
        if (!resource.byID.hasOwnProperty(id)) {
          throw new Error(`Could not find resource [${id}]`)
        }

        return resource.byID[id]
      },
      [resource]
    ),
    add: useCallback(
      (id: DataID<T>, data?: T) => {
        if (data) {
          resource.byID[id] = data
          resource.allIDs.push(id)
          onChange(resource)
          return
        }

        let _data
        if (typeof generator === 'function') {
          _data = generator()
        } else {
          _data = generator
        }

        if (!_data) {
          throw new Error(`No valid data when adding [${id}]`)
        }

        resource.byID[id] = _data
        resource.allIDs.push(id)
        onChange(resource)
      },
      [resource]
    ),
    update: useCallback(
      (id: DataID<T>, data: Partial<T>) => {
        if (!resource.byID.hasOwnProperty(id)) {
          throw new Error(`Could not update resource [${id}]`)
        }

        console.log('resource update', id, data)

        resource.byID = {
          ...resource.byID,
          [id]: {
            ...resource.byID[id],
            ...data,
          },
        }

        onChange(resource)
      },
      [resource]
    ),
    remove: useCallback(
      (id: DataID<T>) => {
        if (!resource.byID.hasOwnProperty(id)) {
          return
        }

        delete resource.byID[id]
        resource.allIDs = resource.allIDs.filter(i => i !== id)

        onChange(resource)
      },
      [resource]
    ),

    get allIDs() {
      return resource.allIDs
    },
    get all() {
      return resource.allIDs.map(id => resource.byID[id])
    },

    indexOf: useCallback(
      (id: DataID<T>): number => {
        return resource.allIDs.indexOf(id)
      },
      [resource]
    ),
    move: useCallback(
      (_id: DataID<T>, _index: number) => {
        throw new Error('Not implemented')
      },
      [resource]
    ),
  }
}

export default useResource
