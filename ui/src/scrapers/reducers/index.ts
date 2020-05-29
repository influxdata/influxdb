// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, ResourceState, Scraper, ResourceType} from 'src/types'
import {
  Action,
  SET_SCRAPERS,
  ADD_SCRAPER,
  EDIT_SCRAPER,
  REMOVE_SCRAPER,
} from 'src/scrapers/actions/creators'

// Utils
import {
  setResource,
  editResource,
  addResource,
  removeResource,
} from 'src/resources/reducers/helpers'

type ScrapersState = ResourceState['scrapers']
const {Scrapers} = ResourceType

const initialState = (): ScrapersState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

export const scrapersReducer = (
  state: ScrapersState = initialState(),
  action: Action
): ScrapersState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_SCRAPERS: {
        setResource<Scraper>(draftState, action, Scrapers)

        return
      }

      case ADD_SCRAPER: {
        addResource<Scraper>(draftState, action, Scrapers)

        return
      }

      case EDIT_SCRAPER: {
        editResource<Scraper>(draftState, action, Scrapers)

        return
      }

      case REMOVE_SCRAPER: {
        removeResource<Scraper>(draftState, action)

        return
      }
    }
  })
