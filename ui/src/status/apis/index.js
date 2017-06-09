// import {get} from 'utils/ajax'

import {fixtureJSONFeed} from 'src/status/fixtures'
// TODO: remove async/await & object return, uncomment get(url) when proxy route implemented
// export const fetchJSONFeed = async url => {
export const fetchJSONFeed = async () => {
  return await {data: fixtureJSONFeed}
  // get(url)
}
