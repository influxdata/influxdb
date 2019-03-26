// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/scrapers/actions'
import {ScraperTargetResponse} from '@influxdata/influx'

const initialState = (): ScrapersState => ({
  status: RemoteDataState.NotStarted,
  list: [],
})

export interface ScrapersState {
  status: RemoteDataState
  list: ScraperTargetResponse[]
}

export const scrapersReducer = (
  state: ScrapersState = initialState(),
  action: Action
): ScrapersState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_SCRAPERS': {
        const {status, list} = action.payload

        draftState.status = status

        if (list) {
          draftState.list = list
        }

        return
      }

      case 'ADD_SCRAPER': {
        const {scraper} = action.payload

        draftState.list.push(scraper)

        return
      }

      case 'EDIT_SCRAPER': {
        const {scraper} = action.payload
        const {list} = draftState

        draftState.list = list.map(l => {
          if (l.id === scraper.id) {
            return scraper
          }

          return l
        })

        return
      }

      case 'REMOVE_SCRAPER': {
        const {id} = action.payload
        const {list} = draftState
        const deleted = list.filter(l => {
          return l.id !== id
        })

        draftState.list = deleted
        return
      }
    }
  })
