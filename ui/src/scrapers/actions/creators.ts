// Types
import {RemoteDataState, ScraperEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export type Action =
  | ReturnType<typeof setScrapers>
  | ReturnType<typeof addScraper>
  | ReturnType<typeof editScraper>
  | ReturnType<typeof removeScraper>

export const SET_SCRAPERS = 'SET_SCRAPERS'
export const ADD_SCRAPER = 'ADD_SCRAPERS'
export const EDIT_SCRAPER = 'EDIT_SCRAPERS'
export const REMOVE_SCRAPER = 'REMOVE_SCRAPERS'

export const setScrapers = (
  status: RemoteDataState,
  schema?: NormalizedSchema<ScraperEntities, string[]>
) =>
  ({
    type: SET_SCRAPERS,
    status,
    schema,
  } as const)

export const addScraper = (schema: NormalizedSchema<ScraperEntities, string>) =>
  ({
    type: ADD_SCRAPER,
    schema,
  } as const)

export const editScraper = (
  schema: NormalizedSchema<ScraperEntities, string>
) =>
  ({
    type: EDIT_SCRAPER,
    schema,
  } as const)

export const removeScraper = (id: string) =>
  ({
    type: REMOVE_SCRAPER,
    id,
  } as const)
