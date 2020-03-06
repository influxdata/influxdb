// Libraries
import {normalize} from 'normalizr'

// API
import {client} from 'src/utils/api'

// Schemas
import {arrayOfScrapers, scraperSchema} from 'src/schemas'

// Types
import {RemoteDataState, GetState, Scraper, ScraperEntities} from 'src/types'
import {Dispatch} from 'react'

// Actions
import {
  Action as ScraperAction,
  setScrapers,
  addScraper,
  editScraper,
  removeScraper,
} from 'src/scrapers/actions/creators'
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'

import {
  scraperCreateFailed,
  scraperCreateSuccess,
  scraperDeleteFailed,
  scraperDeleteSuccess,
  scraperUpdateFailed,
  scraperUpdateSuccess,
} from 'src/shared/copy/notifications'

// Selectors
import {getOrg} from 'src/organizations/selectors'

type Action = ScraperAction | NotifyAction

export const getScrapers = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())

    dispatch(setScrapers(RemoteDataState.Loading))

    const resp = await client.scrapers.getAll(org.id)

    const normalized = normalize<Scraper, ScraperEntities, string[]>(
      resp,
      arrayOfScrapers
    )

    dispatch(setScrapers(RemoteDataState.Done, normalized))
  } catch (error) {
    console.error(error)
    dispatch(setScrapers(RemoteDataState.Error))
  }
}

export const createScraper = (scraper: Scraper) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await client.scrapers.create(scraper)

    const normalized = normalize<Scraper, ScraperEntities, string>(
      resp,
      scraperSchema
    )

    dispatch(addScraper(normalized))
    dispatch(notify(scraperCreateSuccess()))
  } catch (error) {
    console.error(error)
    dispatch(notify(scraperCreateFailed()))
  }
}

export const updateScraper = (scraper: Scraper) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await client.scrapers.update(scraper.id, scraper)
    const normalized = normalize<Scraper, ScraperEntities, string>(
      resp,
      scraperSchema
    )

    dispatch(editScraper(normalized))
    dispatch(notify(scraperUpdateSuccess(scraper.name)))
  } catch (error) {
    console.error(error)
    dispatch(notify(scraperUpdateFailed(scraper.name)))
  }
}

export const deleteScraper = (scraper: Scraper) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    await client.scrapers.delete(scraper.id)

    dispatch(removeScraper(scraper.id))
    dispatch(notify(scraperDeleteSuccess(scraper.name)))
  } catch (error) {
    console.error(error)
    dispatch(notify(scraperDeleteFailed(scraper.name)))
  }
}
