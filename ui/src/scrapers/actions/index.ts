// API
import {client} from 'src/utils/api'

// Types
import {RemoteDataState, GetState} from 'src/types'
import {ScraperTargetResponse, ScraperTargetRequest} from '@influxdata/influx'
import {Dispatch} from 'react'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'

import {
  scraperCreateFailed,
  scraperCreateSuccess,
  scraperDeleteFailed,
  scraperDeleteSuccess,
  scraperUpdateFailed,
  scraperUpdateSuccess,
} from 'src/shared/copy/notifications'

export type Action =
  | SetScrapers
  | AddScraper
  | EditScraper
  | RemoveScraper
  | NotifyAction

interface SetScrapers {
  type: 'SET_SCRAPERS'
  payload: {
    status: RemoteDataState
    list: ScraperTargetResponse[]
  }
}

export const setScrapers = (
  status: RemoteDataState,
  list?: ScraperTargetResponse[]
): SetScrapers => ({
  type: 'SET_SCRAPERS',
  payload: {status, list},
})

interface AddScraper {
  type: 'ADD_SCRAPER'
  payload: {
    scraper: ScraperTargetRequest
  }
}

export const addScraper = (scraper: ScraperTargetRequest): AddScraper => ({
  type: 'ADD_SCRAPER',
  payload: {scraper},
})

interface EditScraper {
  type: 'EDIT_SCRAPER'
  payload: {
    scraper: ScraperTargetResponse
  }
}

export const editScraper = (scraper: ScraperTargetResponse): EditScraper => ({
  type: 'EDIT_SCRAPER',
  payload: {scraper},
})

interface RemoveScraper {
  type: 'REMOVE_SCRAPER'
  payload: {id: string}
}

export const removeScraper = (id: string): RemoveScraper => ({
  type: 'REMOVE_SCRAPER',
  payload: {id},
})

export const getScrapers = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const {
      orgs: {org},
    } = getState()

    dispatch(setScrapers(RemoteDataState.Loading))

    const scrapers = await client.scrapers.getAll(org.id)

    dispatch(setScrapers(RemoteDataState.Done, scrapers))
  } catch (e) {
    console.error(e)
    dispatch(setScrapers(RemoteDataState.Error))
  }
}

export const createScraper = (scraper: ScraperTargetRequest) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const createdScraper = await client.scrapers.create(scraper)

    dispatch(addScraper(createdScraper))
    dispatch(notify(scraperCreateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(scraperCreateFailed()))
    throw e
  }
}

export const updateScraper = (scraper: ScraperTargetResponse) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const t = await client.scrapers.update(scraper.id, scraper)

    dispatch(editScraper(t))
    dispatch(notify(scraperUpdateSuccess(scraper.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(scraperUpdateFailed(scraper.name)))
  }
}

export const deleteScraper = (scraper: ScraperTargetResponse) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.scrapers.delete(scraper.id)
    dispatch(removeScraper(scraper.id))

    dispatch(notify(scraperDeleteSuccess(scraper.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(scraperDeleteFailed(scraper.name)))
  }
}
