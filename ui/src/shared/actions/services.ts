import {Source, Service} from 'src/types'

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
