import {ScraperTargetRequest} from '@influxdata/influx'

// API
import {client} from 'src/utils/api'

export const createScraper = (scraper: ScraperTargetRequest) => async () => {
  await client.scrapers.create(scraper)
}
