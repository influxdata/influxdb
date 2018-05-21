import {Source, Service, NewService} from 'src/types'
import {
  updateService as updateServiceAJAX,
  getServices as getServicesAJAX,
  createService as createServiceAJAX,
} from 'src/shared/apis'
import {notify} from './notifications'
import {couldNotGetServices} from 'src/shared/copy/notifications'

export type Action =
  | ActionLoadServices
  | ActionAddService
  | ActionDeleteService
  | ActionUpdateService
  | ActionSetActiveService

// Load Services
export type LoadServices = (services: Service[]) => ActionLoadServices
export interface ActionLoadServices {
  type: 'LOAD_SERVICES'
  payload: {
    services: Service[]
  }
}

export const loadServices = (services: Service[]): ActionLoadServices => ({
  type: 'LOAD_SERVICES',
  payload: {
    services,
  },
})

// Add a Service
export type AddService = (service: Service) => ActionAddService
export interface ActionAddService {
  type: 'ADD_SERVICE'
  payload: {
    service: Service
  }
}

export const addService = (service: Service): ActionAddService => ({
  type: 'ADD_SERVICE',
  payload: {
    service,
  },
})

// Delete Service
export type DeleteService = (service: Service) => ActionDeleteService
export interface ActionDeleteService {
  type: 'DELETE_SERVICE'
  payload: {
    service: Service
  }
}

export const deleteService = (service: Service): ActionDeleteService => ({
  type: 'DELETE_SERVICE',
  payload: {
    service,
  },
})

// Update Service
export type UpdateService = (service: Service) => ActionUpdateService
export interface ActionUpdateService {
  type: 'UPDATE_SERVICE'
  payload: {
    service: Service
  }
}

export const updateService = (service: Service): ActionUpdateService => ({
  type: 'UPDATE_SERVICE',
  payload: {
    service,
  },
})

// Set Active Service
export type SetActiveService = (
  source: Source,
  service: Service
) => ActionSetActiveService
export interface ActionSetActiveService {
  type: 'SET_ACTIVE_SERVICE'
  payload: {
    source: Source
    service: Service
  }
}

export const setActiveService = (
  source: Source,
  service: Service
): ActionSetActiveService => ({
  type: 'SET_ACTIVE_SERVICE',
  payload: {
    source,
    service,
  },
})

export type FetchServicesAsync = (source: Source) => (dispatch) => Promise<void>
export const fetchServicesAsync = (source: Source) => async (
  dispatch
): Promise<void> => {
  try {
    const services = await getServicesAJAX(source.links.services)
    dispatch(loadServices(services))
  } catch (err) {
    dispatch(notify(couldNotGetServices))
  }
}

export type CreateServiceAsync = (
  source: Source,
  service: NewService
) => (dispatch) => Promise<void>

export const createServiceAsync = (
  source: Source,
  service: NewService
) => async (dispatch): Promise<void> => {
  try {
    const s = await createServiceAJAX(source, service)
    dispatch(addService(s))
  } catch (err) {
    console.error(err.data)
    throw err.data
  }
}

export type UpdateServiceAsync = (
  service: Service
) => (dispatch) => Promise<void>
export const updateServiceAsync = (service: Service) => async (
  dispatch
): Promise<void> => {
  try {
    const s = await updateServiceAJAX(service)
    dispatch(updateService(s))
  } catch (err) {
    console.error(err.data)
    throw err.data
  }
}
