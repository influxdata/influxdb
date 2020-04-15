// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Scrapers */

// Defines the schema for the "scrapers" resource

export const scraperSchema = new schema.Entity(ResourceType.Scrapers)
export const arrayOfScrapers = [scraperSchema]
